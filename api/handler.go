package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
)

var (
	airlineMap map[string]*Airline
	airportMap map[string]*Airport
	routeMap   []*Route
)

func main() {

	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(render.SetContentType(render.ContentTypeJSON))

	// Set a timeout value on the request context (ctx)
	r.Use(middleware.Timeout(60 * time.Second))
	loadCsvs()
	// RESTy routes for "articles" resource
	r.Route("/route", func(r chi.Router) {

		r.Get("/", GetRoute)
	})
	http.ListenAndServe(":3333", r)
}

func loadCsvs() {
	airlineMap = map[string]*Airline{}
	go func() {

		airlineChan, airlineErrorChan, airlineErrorStruct, err := LoadAirlineCsv("../data/test/airlines.csv")
		airportChan, airportErrorChan, airportErrorStruct, err := LoadAirportCsv("../data/test/airports.csv")
		routeChan, routeErrorChan, routeErrorStruct, err := LoadRouteCsv("../data/test/routes.csv")
		if err != nil {
			errors.Wrap(err, "failed to read file")
		}
		// Wait for errors
		var errs []struct {
			Error string `json:"error"`
		}
		go func() {
			for err := range airlineErrorChan {
				errs = append(errs, struct {
					Error string `json:"error"`
				}{Error: err.Error()})
			}
			for err := range airportErrorChan {
				errs = append(errs, struct {
					Error string `json:"error"`
				}{Error: err.Error()})
			}
			for err := range routeErrorChan {
				errs = append(errs, struct {
					Error string `json:"error"`
				}{Error: err.Error()})
			}
		}()
		go func() {
			for {
				airline, ok := <-airlineChan
				if !ok {
					return // No more data.
				}
				airlineMap[airline.Name] = airline
			}

		}()
		// Wait until all errors are received
		<-airlineErrorStruct
		if len(errs) > 0 {
			fmt.Printf("%v", errs)
		}
		go func() {

			for {
				airport, ok := <-airportChan
				if !ok {
					return // No more data.
				}
				airportMap[airport.Name] = airport
			}

		}()
		<-airportErrorStruct
		if len(errs) > 0 {
			fmt.Printf("%v", errs)
		}
		go func() {

			for {
				route, ok := <-routeChan
				if !ok {
					return // No more data.
				}
				routeMap = append(routeMap, route)
			}
		}()
		<-routeErrorStruct
		if len(errs) > 0 {
			fmt.Printf("%v", errs)
		}
	}()
}

// func FlightCtx(next http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		routeStart := chi.URLParam(r, "origin")
// 		routeEnd := chi.URLParam(r, "destination")
// 		route, err := dbGetRoutes(routeStart, routeEnd)
// 		if err != nil {
// 			http.Error(w, http.StatusText(404), 404)
// 			return
// 		}
// 		ctx := context.WithValue(r.Context(), "route", route)
// 		next.ServeHTTP(w, r.WithContext(ctx))
// 	})
// }

func GetRoute(w http.ResponseWriter, r *http.Request) {
	origin := chi.URLParam(r, "origin")
	destination := chi.URLParam(r, "destination")

	if origin == "" || destination == "" {
		http.Error(w, http.StatusText(400), 400)
		return
	}
	w.WriteHeader(200)
	payload, _ := GetBytes(routeMap)
	w.Write(payload)

}
func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	fmt.Printf("hola")
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// func NewArticleResponse(article *Article) *ArticleResponse {
// 	resp := &ArticleResponse{Article: article}

// 	if resp.User == nil {
// 		if user, _ := dbGetUser(resp.UserID); user != nil {
// 			resp.User = NewUserPayloadResponse(user)
// 		}
// 	}

// 	return resp
// }
