package notification

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/4olcay/notification/internal/queue"
	"github.com/google/uuid"
)

type mockStore struct {
	createWithIdempotencyFn   func(ctx context.Context, n Notification) (Notification, bool, error)
	createFn                  func(ctx context.Context, n Notification) (Notification, error)
	createBatchAtomicFn       func(ctx context.Context, b Batch, notifications []Notification) (Batch, error)
	findByIDFn                func(ctx context.Context, id string) (Notification, error)
	findBatchWithLiveCountsFn func(ctx context.Context, id string) (Batch, error)
	updateStatusFn            func(ctx context.Context, id string, status Status, errMsg string) error
	cancelFn                  func(ctx context.Context, id string) error
	listFn                    func(ctx context.Context, f ListFilter) ([]Notification, int, error)
}

func (m *mockStore) CreateWithIdempotency(ctx context.Context, n Notification) (Notification, bool, error) {
	return m.createWithIdempotencyFn(ctx, n)
}
func (m *mockStore) Create(ctx context.Context, n Notification) (Notification, error) {
	return m.createFn(ctx, n)
}
func (m *mockStore) CreateBatchAtomic(ctx context.Context, b Batch, notifications []Notification) (Batch, error) {
	if m.createBatchAtomicFn != nil {
		return m.createBatchAtomicFn(ctx, b, notifications)
	}
	return b, nil
}
func (m *mockStore) FindByID(ctx context.Context, id string) (Notification, error) {
	return m.findByIDFn(ctx, id)
}
func (m *mockStore) FindBatchWithLiveCounts(ctx context.Context, id string) (Batch, error) {
	return m.findBatchWithLiveCountsFn(ctx, id)
}
func (m *mockStore) UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error {
	return m.updateStatusFn(ctx, id, status, errMsg)
}
func (m *mockStore) Cancel(ctx context.Context, id string) error {
	return m.cancelFn(ctx, id)
}
func (m *mockStore) List(ctx context.Context, f ListFilter) ([]Notification, int, error) {
	return m.listFn(ctx, f)
}

type mockPublisher struct {
	publishFn func(ctx context.Context, msg queue.Message) error
}

func (m *mockPublisher) Publish(ctx context.Context, msg queue.Message) error {
	return m.publishFn(ctx, msg)
}

func newService(store notifStore, pub queuePublisher) *Service {
	return NewService(store, pub, 5)
}

func okPublisher() *mockPublisher {
	return &mockPublisher{publishFn: func(_ context.Context, _ queue.Message) error { return nil }}
}

func TestCreate_NoIdempotency_Success(t *testing.T) {
	created := Notification{ID: uuid.New().String(), Status: StatusPending}
	store := &mockStore{
		createFn: func(_ context.Context, n Notification) (Notification, error) {
			return created, nil
		},
		updateStatusFn: func(_ context.Context, _ string, _ Status, _ string) error { return nil },
	}

	svc := newService(store, okPublisher())
	n, isNew, err := svc.Create(context.Background(), CreateRequest{
		Recipient: "test@example.com",
		Channel:   "email",
		Content:   "Hello",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isNew {
		t.Error("expected isNew=true")
	}
	if n.ID != created.ID {
		t.Errorf("expected id %s, got %s", created.ID, n.ID)
	}
}

func TestCreate_WithIdempotency_New(t *testing.T) {
	key := "key-abc"
	created := Notification{ID: uuid.New().String(), IdempotencyKey: &key}
	store := &mockStore{
		createWithIdempotencyFn: func(_ context.Context, n Notification) (Notification, bool, error) {
			return created, true, nil
		},
		updateStatusFn: func(_ context.Context, _ string, _ Status, _ string) error { return nil },
	}

	svc := newService(store, okPublisher())
	n, isNew, err := svc.Create(context.Background(), CreateRequest{
		Recipient:      "user",
		Channel:        "sms",
		Content:        "Hi",
		IdempotencyKey: &key,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isNew {
		t.Error("expected isNew=true for new idempotent request")
	}
	if n.ID != created.ID {
		t.Errorf("id mismatch: got %s", n.ID)
	}
}

func TestCreate_WithIdempotency_Duplicate(t *testing.T) {
	key := "dup-key"
	existing := Notification{ID: uuid.New().String(), IdempotencyKey: &key, Status: StatusDelivered}
	store := &mockStore{
		createWithIdempotencyFn: func(_ context.Context, _ Notification) (Notification, bool, error) {
			return existing, false, nil
		},
	}

	svc := newService(store, okPublisher())
	n, isNew, err := svc.Create(context.Background(), CreateRequest{
		Recipient:      "user",
		Channel:        "push",
		Content:        "Dup",
		IdempotencyKey: &key,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isNew {
		t.Error("expected isNew=false for duplicate idempotent request")
	}
	if n.ID != existing.ID {
		t.Errorf("expected existing id %s, got %s", existing.ID, n.ID)
	}
}

func TestCreate_ContentValidation_SMSTooLong(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	content := make([]byte, 1521)
	for i := range content {
		content[i] = 'x'
	}
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   string(content),
	})
	if err == nil {
		t.Fatal("expected validation error for oversized SMS")
	}
}

func TestCreate_ContentValidation_InvalidEmail(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient: "not-an-email",
		Channel:   "email",
		Content:   "body",
	})
	if err == nil {
		t.Fatal("expected validation error for invalid email recipient")
	}
}

func TestCreate_PushTooLong(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	content := make([]byte, 257)
	for i := range content {
		content[i] = 'a'
	}
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient: "device-token",
		Channel:   "push",
		Content:   string(content),
	})
	if err == nil {
		t.Fatal("expected validation error for oversized push payload")
	}
}

func TestCreate_IdempotencyKeyTooLong(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	key := string(make([]byte, 256))
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient:      "user",
		Channel:        "push",
		Content:        "hi",
		IdempotencyKey: &key,
	})
	if err == nil {
		t.Fatal("expected validation error for idempotency_key > 255 chars")
	}
}

func TestCreate_IdempotencyKeyEmpty(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	empty := ""
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient:      "user",
		Channel:        "push",
		Content:        "hi",
		IdempotencyKey: &empty,
	})
	if err == nil {
		t.Fatal("expected validation error for empty idempotency_key")
	}
}

func TestCreateBatch_DuplicateIdempotencyKey(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	key := "same-key"
	_, err := svc.CreateBatch(context.Background(), CreateBatchRequest{
		Notifications: []CreateRequest{
			{Recipient: "+1111", Channel: "sms", Content: "a", IdempotencyKey: &key},
			{Recipient: "+2222", Channel: "sms", Content: "b", IdempotencyKey: &key},
		},
	})
	if err == nil {
		t.Fatal("expected error for duplicate idempotency_key within same batch")
	}
}

func TestCreate_Scheduled_NotEnqueued(t *testing.T) {
	publishCalled := false
	created := Notification{ID: uuid.New().String()}
	store := &mockStore{
		createFn: func(_ context.Context, _ Notification) (Notification, error) { return created, nil },
	}
	pub := &mockPublisher{publishFn: func(_ context.Context, _ queue.Message) error {
		publishCalled = true
		return nil
	}}

	future := time.Now().Add(time.Hour)
	svc := newService(store, pub)
	_, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient:   "user",
		Channel:     "push",
		Content:     "later",
		ScheduledAt: &future,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if publishCalled {
		t.Error("scheduled notification must not be enqueued immediately")
	}
}

func TestCreateBatch_ValidationFailure(t *testing.T) {
	svc := newService(&mockStore{}, okPublisher())
	_, err := svc.CreateBatch(context.Background(), CreateBatchRequest{
		Notifications: []CreateRequest{
			{Recipient: "ok@example.com", Channel: "email", Content: "fine"},
			{Recipient: "bad-email", Channel: "email", Content: "broken"},
		},
	})
	if err == nil {
		t.Fatal("expected validation error from second notification")
	}
}

func TestCreateBatch_Success(t *testing.T) {
	batchID := uuid.New().String()
	liveBatch := Batch{ID: batchID, Total: 2, Pending: 2}
	store := &mockStore{
		createBatchAtomicFn: func(_ context.Context, b Batch, _ []Notification) (Batch, error) {
			return Batch{ID: batchID, Total: b.Total}, nil
		},
		findBatchWithLiveCountsFn: func(_ context.Context, id string) (Batch, error) {
			return liveBatch, nil
		},
		updateStatusFn: func(_ context.Context, _ string, _ Status, _ string) error { return nil },
	}

	svc := newService(store, okPublisher())
	batch, err := svc.CreateBatch(context.Background(), CreateBatchRequest{
		Notifications: []CreateRequest{
			{Recipient: "+1111", Channel: "sms", Content: "msg1"},
			{Recipient: "+2222", Channel: "sms", Content: "msg2"},
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch.ID != batchID {
		t.Errorf("expected batch id %s, got %s", batchID, batch.ID)
	}
	if batch.Pending != 2 {
		t.Errorf("expected pending=2 from live counts, got %d", batch.Pending)
	}
}

func TestGetBatchByID_Found(t *testing.T) {
	b := Batch{ID: uuid.New().String(), Total: 5, Delivered: 3, Failed: 2}
	store := &mockStore{
		findBatchWithLiveCountsFn: func(_ context.Context, id string) (Batch, error) {
			return b, nil
		},
	}

	svc := newService(store, okPublisher())
	got, err := svc.GetBatchByID(context.Background(), b.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Delivered != 3 {
		t.Errorf("expected delivered=3, got %d", got.Delivered)
	}
}

func TestGetBatchByID_NotFound(t *testing.T) {
	store := &mockStore{
		findBatchWithLiveCountsFn: func(_ context.Context, _ string) (Batch, error) {
			return Batch{}, ErrNotFound
		},
	}

	svc := newService(store, okPublisher())
	_, err := svc.GetBatchByID(context.Background(), "missing-id")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestCancel_Success(t *testing.T) {
	store := &mockStore{
		cancelFn: func(_ context.Context, _ string) error { return nil },
	}
	svc := newService(store, okPublisher())
	if err := svc.Cancel(context.Background(), uuid.New().String()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCancel_CannotCancel(t *testing.T) {
	store := &mockStore{
		cancelFn: func(_ context.Context, _ string) error { return ErrCannotCancel },
	}
	svc := newService(store, okPublisher())
	err := svc.Cancel(context.Background(), uuid.New().String())
	if !errors.Is(err, ErrCannotCancel) {
		t.Errorf("expected ErrCannotCancel, got %v", err)
	}
}

func TestCancel_NotFound(t *testing.T) {
	store := &mockStore{
		cancelFn: func(_ context.Context, _ string) error { return ErrNotFound },
	}
	svc := newService(store, okPublisher())
	err := svc.Cancel(context.Background(), "non-existent-id")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for non-existent notification, got %v", err)
	}
}
