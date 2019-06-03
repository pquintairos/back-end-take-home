package main

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type Airline struct {
	Name       string `json:"name,omitempty"`
	Code2Digit string `json:"code2Digit,omitempty"`
	Code3Digit string `json:"code3Digit,omitempty"`
	Country    string `json:"country,omitempty"`
}

type Airport struct {
	Name      string `json:"name,omitempty"`
	City      string `json:"city,omitempty"`
	Country   string `json:"country,omitempty"`
	IATA3     string `json:"iata3,omitempty"`
	Latitute  string `json:"latitude,omitempty"`
	Longitude string `json:"longitude,omitempty"`
}

type Route struct {
	AirlineID   string `json:"airineId,omitempty"`
	Origin      string `json:"origin,omitempty"`
	Destination string `json:"destination,omitempty"`
}

func LoadAirlineCsv(fileLocation string) (chan *Airline, chan error, chan struct{}, error) {
	f, err := os.Open(fileLocation)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read file")
	}
	defer f.Close() // this needs to be after the err check

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true

	csvHeader, err := reader.Read()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read CSV header")
	}

	headersFound := map[string]bool{}
	for i, h := range csvHeader {
		csvHeader[i] = strings.ToLower(h) // lowercase
		headersFound[csvHeader[i]] = true
	}
	done := make(chan struct{})
	airlineRows := make(chan *Airline)
	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(airlineRows)
			close(errCh)
			close(done)
		}()

		for {

			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- errors.Wrap(err, "failed to read CSV record")
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				airlineRow, err := parseAirlineRow(record, csvHeader)
				if err != nil {
					errCh <- errors.Wrap(err, "failed to parse CSV record")
					return
				}

				airlineRows <- airlineRow
			}()
		}
	}()

	return airlineRows, errCh, done, nil
}

func parseAirlineRow(records []string, headers []string) (*Airline, error) {
	if len(headers) != len(records) {
		return nil, errors.Errorf("mismatch between headers (%d) and records (%d)", len(headers), len(records))
	}
	var r Airline
	for i, field := range records {
		switch headers[i] {
		case "name":
			r.Name = parseCell(field)
		case "2 digit code":
			r.Code2Digit = parseCell(field)
		case "3 digit code":
			r.Code3Digit = parseCell(field)
		case "country":
			r.Country = parseCell(field)
		default:
			continue // continue next record field
		}
	}
	return &r, nil
}

func LoadAirportCsv(fileLocation string) (chan *Airport, chan error, chan struct{}, error) {
	f, err := os.Open(fileLocation)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read file")
	}
	defer f.Close() // this needs to be after the err check

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true

	csvHeader, err := reader.Read()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read CSV header")
	}

	headersFound := map[string]bool{}
	for i, h := range csvHeader {
		csvHeader[i] = strings.ToLower(h) // lowercase
		headersFound[csvHeader[i]] = true
	}
	done := make(chan struct{})
	airportRows := make(chan *Airport)
	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(airportRows)
			close(errCh)
			close(done)
		}()

		for {

			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- errors.Wrap(err, "failed to read CSV record")
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				airportRow, err := parseAirportRow(record, csvHeader)
				if err != nil {
					errCh <- errors.Wrap(err, "failed to parse CSV record")
					return
				}

				airportRows <- airportRow
			}()
		}
	}()

	return airportRows, errCh, done, nil
}

func parseAirportRow(records []string, headers []string) (*Airport, error) {
	if len(headers) != len(records) {
		return nil, errors.Errorf("mismatch between headers (%d) and records (%d)", len(headers), len(records))
	}
	var r Airport
	for i, field := range records {
		switch headers[i] {
		case "name":
			r.Name = parseCell(field)
		case "city":
			r.City = parseCell(field)
		case "iata 3":
			r.IATA3 = parseCell(field)
		case "country":
			r.Country = parseCell(field)
		case "latitude":
			r.Latitute = parseCell(field)
		case "longitude":
			r.Longitude = parseCell(field)
		default:
			continue // continue next record field
		}
	}
	return &r, nil
}

func LoadRouteCsv(fileLocation string) (chan *Route, chan error, chan struct{}, error) {
	f, err := os.Open(fileLocation)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read file")
	}
	defer f.Close() // this needs to be after the err check

	reader := csv.NewReader(f)
	reader.TrimLeadingSpace = true

	csvHeader, err := reader.Read()
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to read CSV header")
	}

	headersFound := map[string]bool{}
	for i, h := range csvHeader {
		csvHeader[i] = strings.ToLower(h) // lowercase
		headersFound[csvHeader[i]] = true
	}
	done := make(chan struct{})
	routeRows := make(chan *Route)
	errCh := make(chan error)
	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(routeRows)
			close(errCh)
			close(done)
		}()

		for {
			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					return
				}
				errCh <- errors.Wrap(err, "failed to read CSV record")
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				routeRow, err := parseRouteRow(record, csvHeader)
				if err != nil {
					errCh <- errors.Wrap(err, "failed to parse CSV record")
					return
				}

				routeRows <- routeRow
			}()
		}
	}()

	return routeRows, errCh, done, nil
}

func parseRouteRow(records []string, headers []string) (*Route, error) {
	if len(headers) != len(records) {
		return nil, errors.Errorf("mismatch between headers (%d) and records (%d)", len(headers), len(records))
	}
	var r Route
	for i, field := range records {
		switch headers[i] {
		case "airline id":
			r.AirlineID = parseCell(field)
		case "origin":
			r.Origin = parseCell(field)
		case "destination":
			r.Destination = parseCell(field)
		default:
			continue // continue next record field
		}
	}
	return &r, nil
}

func parseCell(in string) string {
	return strings.TrimSpace(in)
}
