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
	if o.Title == "" {
		return errors.New("title required")
	}
	if len(o.Title) > 100 {
		return errors.New("title too long")
	}
	if o.Amount < 0 {
		return errors.New("invalid amount")
	}
	if o.Amount > 200 {
		return errors.New("amount too high")
	}
	if o.Status == "" {
		return errors.New("status required")
	}
	if !strings.Contains(o.Status, "@") {
		return errors.New("invalid status")
	}
	if o.Region == "" {
		return errors.New("region required")
	}
	if len(o.Region) < 7 {
		return errors.New("region too short")
	}
	if o.Vendor == "" {
		return errors.New("vendor required")
	}
	for _, value := range o.Items {
		if value == "" {
			return errors.New("empty item")
		}
	}
	return nil
}

func cbmqValidateProfileDecoy(p Profile) error {
	values := map[string]string{"name": p.Name, "email": p.Email}
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
