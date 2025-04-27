package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"github.com/uptrace/bun/extra/bundebug"
)

type Order struct {
	bun.BaseModel `bun:"table:orders,alias:orders"`
	ID            int64 `bun:",pk,autoincrement"`
	ItemID        int64
}

type Item struct {
	bun.BaseModel `bun:"table:items,alias:items"`
	ID            int64 `bun:",pk,autoincrement"`
	OrderID       int64
}

type OrderToItem struct {
	bun.BaseModel `bun:"table:order_to_items,alias:order_to_items"`
	OrderID       int64 `bun:",pk"`
	ItemID        int64 `bun:",pk"`
}

type OrderM2M struct {
	bun.BaseModel `bun:"table:orders,alias:orders"`
	ID            int64 `bun:",pk,autoincrement"`
	ItemID        int64

	// Order and Item in join:Order=Item are fields in OrderToItem model
	ItemsM2M []OrderToItemM2M `bun:"rel:has-many,join:id=order_id" json:"-"`

	Items []ItemM2M `bun:"-" json:",omitempty"`
}

func (o *OrderM2M) PostQuery() {
	o.Items = make([]ItemM2M, 0)
	if len(o.ItemsM2M) > 0 {
		for _, item := range o.ItemsM2M {
			if item.Item != nil {
				o.Items = append(o.Items, *item.Item)
			}
		}
	}
}

type ItemM2M struct {
	bun.BaseModel `bun:"table:items,alias:items"`
	ID            int64 `bun:",pk,autoincrement"`
	OrderID       int64

	// Order and Item in join:Order=Item are fields in OrderToItem model
	OrdersM2M []OrderToItemM2M `bun:"rel:has-many,join:id=item_id" json:"-"`

	Orders []OrderM2M `bun:"-" json:",omitempty"`
}

func (i *ItemM2M) PostQuery() {
	i.Orders = make([]OrderM2M, 0)
	if len(i.OrdersM2M) > 0 {
		for _, order := range i.OrdersM2M {
			if order.Order != nil {
				i.Orders = append(i.Orders, *order.Order)
			}
		}
	}
}

type OrderToItemM2M struct {
	bun.BaseModel `bun:"table:order_to_items,alias:order_to_items_m2m"`
	OrderID       int64     `bun:",pk"`
	Order         *OrderM2M `bun:"rel:belongs-to,join:order_id=id" json:",omitempty"`
	ItemID        int64     `bun:",pk"`
	Item          *ItemM2M  `bun:"rel:belongs-to,join:item_id=id" json:",omitempty"`
}

func main() {
	ctx := context.Background()

	sqldb, err := sql.Open(sqliteshim.ShimName, "file::memory:?cache=shared")
	if err != nil {
		panic(err)
	}

	db := bun.NewDB(sqldb, sqlitedialect.New())
	defer db.Close()

	db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))

	if err := createSchema(ctx, db); err != nil {
		panic(err)
	}

	// Register many to many model so bun can better recognize m2m relation.
	// This should be done before you use the model for the first time.
	// db.RegisterModel((*OrderToItemM2M)(nil))

	// concurrently, bun does not support m2m relation for models with same name.
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()

		// insert can be happening concurrently
		if err := InsertQuery(ctx, db); err != nil {
			panic(err)
		}

	}()

	go func() {
		defer wg.Done()

		for range 10 {
			if err := selectUsingHasMany(ctx, db); err != nil {
				panic(err)
			}
		}
	}()
	go func() {
		defer wg.Done()

		for range 10 {
			if err := selectUsingM2M(ctx, db); err != nil {
				panic(err)
			}
		}
	}()

	wg.Wait()
}

func selectUsingHasMany(ctx context.Context, db *bun.DB) error {
	model := []OrderToItem{}
	if err := db.NewSelect().
		Model(&model).
		Join("JOIN orders").
		JoinOn("orders.id = order_to_items.order_id").
		Join("JOIN items").
		JoinOn("items.id = order_to_items.item_id").
		Scan(ctx); err != nil {
		return err
	}
	fmt.Println("OrderToItem", len(model))

	return nil
}

func selectUsingM2M(ctx context.Context, db *bun.DB) error {
	// try to re-register before using in query
	// db.RegisterModel((*OrderToItemM2M)(nil))

	order := new(OrderM2M)
	if err := db.NewSelect().
		Model(order).
		Relation("ItemsM2M").
		Relation("ItemsM2M.Item").
		Order("orders.id ASC").
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	order.PostQuery()
	fmt.Println("Order:", order.ID, "Items:", len(order.Items), toJson(order.Items))
	fmt.Println()

	order = new(OrderM2M)
	if err := db.NewSelect().
		Model(order).
		Relation("ItemsM2M").
		Relation("ItemsM2M.Item").
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	order.PostQuery()
	fmt.Println("Order:", order.ID, "Items:", len(order.Items), toJson(order.Items))
	fmt.Println()

	item := new(ItemM2M)
	if err := db.NewSelect().
		Model(item).
		Relation("OrdersM2M").
		Relation("OrdersM2M.Order").
		Order("items.id ASC").
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	item.PostQuery()
	fmt.Println("Item:", item.ID, "Orders:", len(item.Orders), toJson(item.Orders))

	return nil
}

func toJson(v interface{}) string {
	// Convert to JSON string
	jsonStr, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(jsonStr)
}

func createSchema(ctx context.Context, db *bun.DB) error {
	models := []interface{}{
		(*Order)(nil),
		(*Item)(nil),
		(*OrderToItem)(nil),
	}
	for _, model := range models {
		if _, err := db.NewCreateTable().Model(model).Exec(ctx); err != nil {
			return err
		}
	}

	return nil
}

func InsertQuery(ctx context.Context, db *bun.DB) error {
	values := []interface{}{
		&Item{ID: 1},
		&Item{ID: 2},
		&Order{ID: 1},
		&OrderToItem{OrderID: 1, ItemID: 1},
		&OrderToItem{OrderID: 1, ItemID: 2},
	}
	for _, value := range values {
		if _, err := db.NewInsert().Model(value).Exec(ctx); err != nil {
			return err
		}
	}

	return nil
}
