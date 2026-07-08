package main

import (
	"fmt"

	"github.com/fredouric/kvraft/store/persisted"
)

func main() {
	fmt.Println("Hello world!")

	s := persisted.New()
	s.Set("hello", "there")
	s.Set("General", "Kenobi")
	value, _ := s.Get("hello")

	fmt.Println(value)
	s.Delete("hello")
}
