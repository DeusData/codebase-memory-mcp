package cbmq

import (
	"errors"
	"strings"
)

func cbmqValidateUser(u User) error {
	if u.Name == "" {
		return errors.New("name required")
	}
	if len(u.Name) > 100 {
		return errors.New("name too long")
	}
	if u.Age < 0 {
		return errors.New("invalid age")
	}
	if u.Age > 200 {
		return errors.New("age too high")
	}
	if u.Email == "" {
		return errors.New("email required")
	}
	if !strings.Contains(u.Email, "@") {
		return errors.New("invalid email")
	}
	if u.Phone == "" {
		return errors.New("phone required")
	}
	if len(u.Phone) < 7 {
		return errors.New("phone too short")
	}
	if u.Country == "" {
		return errors.New("country required")
	}
	for _, value := range u.Tags {
		if value == "" {
			return errors.New("empty tag")
		}
	}
	return nil
}

func cbmqValidateOrder(o Order) error {
	values := map[string]string{"title": o.Title, "status": o.Status}
	missing := make([]string, 0)
	for field, value := range values {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, field)
		}
	}
	if len(missing) != 0 {
		return errors.New(strings.Join(missing, ","))
	}
	return nil
}

func cbmqValidateProfileDecoy(p Profile) error {
	if p.Name == "" {
		return errors.New("name required")
	}
	if len(p.Name) > 100 {
		return errors.New("name too long")
	}
	if p.Age < 0 {
		return errors.New("invalid age")
	}
	if p.Age > 200 {
		return errors.New("age too high")
	}
	if p.Email == "" {
		return errors.New("email required")
	}
	if !strings.Contains(p.Email, "@") {
		return errors.New("invalid email")
	}
	if p.Phone == "" {
		return errors.New("phone required")
	}
	if len(p.Phone) < 7 {
		return errors.New("phone too short")
	}
	if p.Country == "" {
		return errors.New("country required")
	}
	for _, value := range p.Tags {
		if value == "" {
			return errors.New("empty tag")
		}
	}
	return nil
}
