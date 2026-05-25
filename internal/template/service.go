package template

import (
	"context"
	"strings"

	"github.com/google/uuid"
)

type store interface {
	Create(ctx context.Context, t Template) (Template, error)
	FindByName(ctx context.Context, name string) (Template, error)
	List(ctx context.Context) ([]Template, error)
	Delete(ctx context.Context, name string) error
}

type Service struct {
	repo store
}

func NewService(repo store) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (Template, error) {
	t := Template{
		ID:      uuid.New().String(),
		Name:    req.Name,
		Channel: req.Channel,
		Subject: req.Subject,
		Body:    req.Body,
	}
	return s.repo.Create(ctx, t)
}

func (s *Service) Render(ctx context.Context, name string, req RenderRequest) (RenderResponse, error) {
	t, err := s.repo.FindByName(ctx, name)
	if err != nil {
		return RenderResponse{}, err
	}

	body := substituteVars(t.Body, req.Variables)
	resp := RenderResponse{Body: body}
	if t.Subject != nil {
		sub := substituteVars(*t.Subject, req.Variables)
		resp.Subject = &sub
	}
	return resp, nil
}

func (s *Service) List(ctx context.Context) ([]Template, error) {
	return s.repo.List(ctx)
}

func (s *Service) Delete(ctx context.Context, name string) error {
	return s.repo.Delete(ctx, name)
}

func substituteVars(text string, vars map[string]string) string {
	if len(vars) == 0 {
		return text
	}
	pairs := make([]string, 0, len(vars)*2)
	for k, v := range vars {
		pairs = append(pairs, "{{"+k+"}}", v)
	}
	return strings.NewReplacer(pairs...).Replace(text)
}
