package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

// setupBenchRepo creates a temp directory with 10 Go files across 2 packages.
// Package "model" defines structs and methods; package "handler" calls into model.
func setupBenchRepo(b *testing.B) (dir string, cleanup func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "cgm-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	cleanup = func() { os.RemoveAll(dir) }

	writeBenchModelFiles(b, dir)
	writeBenchHandlerFiles(b, dir)

	return dir, cleanup
}

func writeBenchModelFiles(b *testing.B, dir string) {
	b.Helper()

	writeBenchFile(b, filepath.Join(dir, "model", "order.go"), `package model

type Order struct {
	ID       string
	Customer string
	Total    float64
	Items    []Item
}

func NewOrder(id, customer string) *Order {
	return &Order{ID: id, Customer: customer}
}

func (o *Order) AddItem(item Item) {
	o.Items = append(o.Items, item)
	o.Total += item.Price
}

func (o *Order) Validate() error {
	if o.ID == "" {
		return nil
	}
	return nil
}
`)

	writeBenchFile(b, filepath.Join(dir, "model", "item.go"), `package model

type Item struct {
	SKU   string
	Name  string
	Price float64
	Qty   int
}

func NewItem(sku, name string, price float64, qty int) Item {
	return Item{SKU: sku, Name: name, Price: price, Qty: qty}
}

func (i Item) Subtotal() float64 {
	return i.Price * float64(i.Qty)
}
`)

	writeBenchFile(b, filepath.Join(dir, "model", "customer.go"), `package model

type Customer struct {
	ID    string
	Name  string
	Email string
}

func NewCustomer(id, name, email string) *Customer {
	return &Customer{ID: id, Name: name, Email: email}
}

func (c *Customer) DisplayName() string {
	if c.Name != "" {
		return c.Name
	}
	return c.Email
}
`)

	writeBenchFile(b, filepath.Join(dir, "model", "inventory.go"), `package model

type Inventory struct {
	Stock map[string]int
}

func NewInventory() *Inventory {
	return &Inventory{Stock: make(map[string]int)}
}

func (inv *Inventory) Reserve(sku string, qty int) bool {
	if inv.Stock[sku] >= qty {
		inv.Stock[sku] -= qty
		return true
	}
	return false
}

func (inv *Inventory) Restock(sku string, qty int) {
	inv.Stock[sku] += qty
}
`)

	writeBenchFile(b, filepath.Join(dir, "model", "pricing.go"), `package model

const (
	TaxRate     = 0.19
	DiscountCap = 0.25
)

func ApplyTax(amount float64) float64 {
	return amount * (1 + TaxRate)
}

func ApplyDiscount(amount, discount float64) float64 {
	if discount > DiscountCap {
		discount = DiscountCap
	}
	return amount * (1 - discount)
}

func FinalPrice(amount, discount float64) float64 {
	return ApplyTax(ApplyDiscount(amount, discount))
}
`)
}

func writeBenchHandlerFiles(b *testing.B, dir string) {
	b.Helper()

	writeBenchFile(b, filepath.Join(dir, "handler", "order_handler.go"), `package handler

import "example.com/bench/model"

func CreateOrder(customerID string, items []model.Item) (*model.Order, error) {
	c := model.NewCustomer(customerID, "", "")
	order := model.NewOrder("ord-1", c.DisplayName())
	for _, item := range items {
		order.AddItem(item)
	}
	if err := order.Validate(); err != nil {
		return nil, err
	}
	return order, nil
}

func GetOrderTotal(order *model.Order) float64 {
	return model.ApplyTax(order.Total)
}
`)

	writeBenchFile(b, filepath.Join(dir, "handler", "inventory_handler.go"), `package handler

import "example.com/bench/model"

func CheckAndReserve(inv *model.Inventory, sku string, qty int) bool {
	return inv.Reserve(sku, qty)
}

func RestockItem(inv *model.Inventory, sku string, qty int) {
	inv.Restock(sku, qty)
}
`)

	writeBenchFile(b, filepath.Join(dir, "handler", "pricing_handler.go"), `package handler

import "example.com/bench/model"

func CalculatePrice(amount, discount float64) float64 {
	return model.FinalPrice(amount, discount)
}

func QuotePrice(items []model.Item, discount float64) float64 {
	var total float64
	for _, item := range items {
		total += item.Subtotal()
	}
	return model.ApplyDiscount(total, discount)
}
`)

	writeBenchFile(b, filepath.Join(dir, "handler", "customer_handler.go"), `package handler

import "example.com/bench/model"

func RegisterCustomer(id, name, email string) *model.Customer {
	return model.NewCustomer(id, name, email)
}

func CustomerLabel(c *model.Customer) string {
	return c.DisplayName()
}
`)

	writeBenchFile(b, filepath.Join(dir, "handler", "admin_handler.go"), `package handler

import "example.com/bench/model"

func AdminRestock(inv *model.Inventory, sku string, qty int) {
	inv.Restock(sku, qty)
}

func AdminCreateOrder(custName string) *model.Order {
	order := model.NewOrder("admin-1", custName)
	item := model.NewItem("ADMIN-SKU", "Admin Item", 99.99, 1)
	order.AddItem(item)
	return order
}
`)
}

func writeBenchFile(b *testing.B, path, content string) {
	b.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPipelineRun(b *testing.B) {
	repoDir, cleanup := setupBenchRepo(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s, err := store.OpenMemory()
		if err != nil {
			b.Fatal(err)
		}
		p := New(context.Background(), s, repoDir)
		if err := p.Run(); err != nil {
			b.Fatalf("Pipeline.Run: %v", err)
		}
		s.Close()
	}
}

func BenchmarkPipelineReindex(b *testing.B) {
	repoDir, cleanup := setupBenchRepo(b)
	defer cleanup()

	s, err := store.OpenMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	// Initial index
	p := New(context.Background(), s, repoDir)
	if err := p.Run(); err != nil {
		b.Fatalf("initial index: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p := New(context.Background(), s, repoDir)
		if err := p.Run(); err != nil {
			b.Fatalf("reindex %d: %v", i, err)
		}
	}
}

func BenchmarkPipelineRunScaled(b *testing.B) {
	for _, fileCount := range []int{5, 20, 50} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "cgm-bench-scale-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)

			for i := 0; i < fileCount; i++ {
				pkg := fmt.Sprintf("pkg%d", i%5)
				name := fmt.Sprintf("file%d.go", i)
				content := fmt.Sprintf(`package %s

func Func%d() int {
	return %d
}

func Caller%d() int {
	return Func%d()
}

type Struct%d struct {
	Field int
}

func (s *Struct%d) Method%d() int {
	return s.Field + Func%d()
}
`, pkg, i, i, i, i, i, i, i, i)
				writeBenchFile(b, filepath.Join(dir, pkg, name), content)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				s, err := store.OpenMemory()
				if err != nil {
					b.Fatal(err)
				}
				p := New(context.Background(), s, dir)
				if err := p.Run(); err != nil {
					b.Fatalf("Pipeline.Run: %v", err)
				}
				s.Close()
			}
		})
	}
}
