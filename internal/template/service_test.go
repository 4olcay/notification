package template

import (
	"context"
	"errors"
	"testing"
)

type mockStore struct {
	createFn     func(ctx context.Context, t Template) (Template, error)
	findByNameFn func(ctx context.Context, name string) (Template, error)
	listFn       func(ctx context.Context) ([]Template, error)
	deleteFn     func(ctx context.Context, name string) error
}

func (m *mockStore) Create(ctx context.Context, t Template) (Template, error) {
	return m.createFn(ctx, t)
}
func (m *mockStore) FindByName(ctx context.Context, name string) (Template, error) {
	return m.findByNameFn(ctx, name)
}
func (m *mockStore) List(ctx context.Context) ([]Template, error) {
	return m.listFn(ctx)
}
func (m *mockStore) Delete(ctx context.Context, name string) error {
	return m.deleteFn(ctx, name)
}

func newSvc(s store) *Service { return NewService(s) }

func TestSubstituteVars_AllReplaced(t *testing.T) {
	result := substituteVars("Hello {{name}}, your order is {{order_id}}.", map[string]string{
		"name":     "Alice",
		"order_id": "ORD-42",
	})
	want := "Hello Alice, your order is ORD-42."
	if result != want {
		t.Errorf("got %q, want %q", result, want)
	}
}

func TestSubstituteVars_UnknownPlaceholderKept(t *testing.T) {
	result := substituteVars("Hello {{name}}, your code is {{unknown}}.", map[string]string{
		"name": "Bob",
	})
	want := "Hello Bob, your code is {{unknown}}."
	if result != want {
		t.Errorf("got %q, want %q", result, want)
	}
}

func TestSubstituteVars_EmptyVars(t *testing.T) {
	body := "No variables here."
	if got := substituteVars(body, nil); got != body {
		t.Errorf("expected unchanged body, got %q", got)
	}
}

func TestSubstituteVars_MultipleOccurrences(t *testing.T) {
	result := substituteVars("{{x}} and {{x}}", map[string]string{"x": "Y"})
	want := "Y and Y"
	if result != want {
		t.Errorf("got %q, want %q", result, want)
	}
}

func TestCreate_Success(t *testing.T) {
	store := &mockStore{
		createFn: func(_ context.Context, t Template) (Template, error) {
			return t, nil
		},
	}
	svc := newSvc(store)
	tpl, err := svc.Create(context.Background(), CreateRequest{
		Name:    "welcome",
		Channel: "sms",
		Body:    "Hi {{name}}!",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tpl.Name != "welcome" {
		t.Errorf("expected name=welcome, got %s", tpl.Name)
	}
	if tpl.ID == "" {
		t.Error("expected a non-empty ID to be generated")
	}
}

func TestCreate_AlreadyExists(t *testing.T) {
	store := &mockStore{
		createFn: func(_ context.Context, _ Template) (Template, error) {
			return Template{}, ErrAlreadyExists
		},
	}
	svc := newSvc(store)
	_, err := svc.Create(context.Background(), CreateRequest{Name: "dup", Channel: "push", Body: "x"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestRender_Success(t *testing.T) {
	stored := Template{Name: "order", Channel: "email", Body: "Order {{id}} confirmed."}
	store := &mockStore{
		findByNameFn: func(_ context.Context, _ string) (Template, error) { return stored, nil },
	}
	svc := newSvc(store)
	resp, err := svc.Render(context.Background(), "order", RenderRequest{
		Variables: map[string]string{"id": "ORD-99"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "Order ORD-99 confirmed."
	if resp.Body != want {
		t.Errorf("got %q, want %q", resp.Body, want)
	}
}

func TestRender_WithSubject(t *testing.T) {
	sub := "Your order {{id}}"
	stored := Template{Name: "email_tpl", Channel: "email", Subject: &sub, Body: "Details for {{id}}."}
	store := &mockStore{
		findByNameFn: func(_ context.Context, _ string) (Template, error) { return stored, nil },
	}
	svc := newSvc(store)
	resp, err := svc.Render(context.Background(), "email_tpl", RenderRequest{
		Variables: map[string]string{"id": "ORD-7"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Subject == nil {
		t.Fatal("expected Subject to be set")
	}
	if *resp.Subject != "Your order ORD-7" {
		t.Errorf("subject got %q", *resp.Subject)
	}
	if resp.Body != "Details for ORD-7." {
		t.Errorf("body got %q", resp.Body)
	}
}

func TestRender_NotFound(t *testing.T) {
	store := &mockStore{
		findByNameFn: func(_ context.Context, _ string) (Template, error) {
			return Template{}, ErrNotFound
		},
	}
	svc := newSvc(store)
	_, err := svc.Render(context.Background(), "missing", RenderRequest{})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestList_ReturnsList(t *testing.T) {
	templates := []Template{{Name: "a"}, {Name: "b"}}
	store := &mockStore{
		listFn: func(_ context.Context) ([]Template, error) { return templates, nil },
	}
	svc := newSvc(store)
	got, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 templates, got %d", len(got))
	}
}

func TestDelete_Success(t *testing.T) {
	store := &mockStore{
		deleteFn: func(_ context.Context, _ string) error { return nil },
	}
	if err := newSvc(store).Delete(context.Background(), "welcome"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDelete_NotFound(t *testing.T) {
	store := &mockStore{
		deleteFn: func(_ context.Context, _ string) error { return ErrNotFound },
	}
	err := newSvc(store).Delete(context.Background(), "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
