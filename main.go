package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"github.com/uptrace/bun/extra/bundebug"
)

type Order struct {
	ID     int64 `bun:",pk,autoincrement"`
	ItemID int64
}

type Item struct {
	ID      int64 `bun:",pk,autoincrement"`
	OrderID int64
}

type OrderToItem struct {
	bun.BaseModel `bun:"table:order_to_items,alias:order_to_items"`
	OrderID       int64 `bun:",pk"`
	ItemID        int64 `bun:",pk"`
}

type OrderM2M struct {
	Order `bun:",extend"`

	// Order and Item in join:Order=Item are fields in OrderToItem model
	Items []Item `bun:"m2m:order_to_items,join:Order=Item"`
}

type ItemM2M struct {
	Item `bun:",extend"`

	// Order and Item in join:Order=Item are fields in OrderToItem model
	Orders []Order `bun:"m2m:order_to_items,join:Item=Order"`
}

type OrderToItemM2M struct {
	OrderToItem `bun:",extend"`

	Order *Order `bun:"rel:belongs-to,join:order_id=id"`
	Item  *Item  `bun:"rel:belongs-to,join:item_id=id"`
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
	db.RegisterModel((*OrderToItemM2M)(nil))

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
	model := []OrderToItemM2M{}
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
	db.RegisterModel((*OrderToItemM2M)(nil))

	order := new(OrderM2M)
	if err := db.NewSelect().
		Model(order).
		Relation("Items").
		Order("order.id ASC").
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	fmt.Println("Order", order.ID, "Items", order.Items)
	fmt.Println()

	order = new(OrderM2M)
	if err := db.NewSelect().
		Model(order).
		Relation("Items", func(q *bun.SelectQuery) *bun.SelectQuery {
			q = q.OrderExpr("item.id DESC")
			return q
		}).
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	fmt.Println("Order", order.ID, "Items", order.Items)
	fmt.Println()

	item := new(ItemM2M)
	if err := db.NewSelect().
		Model(item).
		Relation("Orders").
		Order("item.id ASC").
		Limit(1).
		Scan(ctx); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	fmt.Println("Item", item.ID, "Order", item.Orders)

	return nil
}

func createSchema(ctx context.Context, db *bun.DB) error {
	models := []interface{}{
		(*Order)(nil),
		(*Item)(nil),
		(*OrderToItemM2M)(nil),
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
