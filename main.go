package main

import (
	"errors"
	"flag"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	hel "github.com/x72hoor/go-helper"
)

var numbers = [10]string{"০", "১", "২", "৩", "৪", "৫", "৬", "৭", "৮", "৯"}

const url = "https://web.pgcb.gov.bd/view_generations_bn?page="

var db *gorm.DB

// var datas []xData

type xData struct {
	ID       uint `gorm:"column:id;primary_key" json:"id"`
	Year     int
	Month    int
	Day      int
	Time     string
	Produced string // mw
	Load     string // mw
	Less     string
	LoadShed string
	// types
	Gas    string
	Liquid string
	Coal   string
	Hydro  string
	Solar  string
	// location
	Veramara string
	Tripura  string
	// other
	Comment string
}

var shouldFetch bool

var sortBy string
var minYear int
var minMonth int
var maxYear int
var maxMonth int
var serverMaxPage int
var outfile string

func main() {
	flags()
	if shouldFetch || !hel.FileExists("db.sqlite") {
		fetchData()
	}
	setDB()
	csv()
}

func flags() {
	flag.BoolVar(&shouldFetch, "ff", false, "force fetch data")
	flag.StringVar(&sortBy, "sb", "desc", "year sort by, values: asc or desc")
	flag.StringVar(&outfile, "o", "", "(required) output file")
	flag.IntVar(&minYear, "min-y", 2017, "min year")
	flag.IntVar(&minMonth, "min-m", 1, "min month")
	flag.IntVar(&maxYear, "max-y", 2020, "min year")
	flag.IntVar(&maxMonth, "max-m", 12, "max month")
	flag.IntVar(&serverMaxPage, "smp", 960, "server's max page")
	flag.Parse()
	if outfile == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func csv() {

	hel.Pl("Generating csv")

	var datas []xData
	var rowsCount int

	db.Model(&xData{}).Count(&rowsCount)

	q := db.Where("(year >= ? AND year <= ?) AND (month >= ? AND month <= ?)", minYear, maxYear, minMonth, maxMonth)

	sortStr := "year " + sortBy + ", month " + sortBy + ", day asc, time"

	q = q.Order(sortStr).Find(&datas)

	hel.Pl("Total rows", rowsCount)

	var out = "Date,Time,Load\n"

	for _, d := range datas {
		if strings.Contains(d.Time, ":30") {
			continue
		}
		date := timify(d.Year) + "/" + timify(d.Month) + "/" + timify(d.Day)
		out += date + "," + strings.ReplaceAll(timeFormat(d.Time), "24:00", "00:00") + "," + d.Load + "\n"
	}

	hel.StrToFile(outfile, out)
}

// ArrStrContainsPartial check whether a sub string (partial) contains
// in any elements in the array
func ArrStrContainsPartial(array []string, substr string) bool {
	var contains = false
	for _, a := range array {
		if strings.Contains(a, substr) {
			contains = true
			break
		}
	}
	return contains
}
func fetchData() {

	hel.FileRemoveIfExists("db.sqlite")

	setDB()

	const minPage = 1

	var wg sync.WaitGroup
	var c = make(chan int, 10)

	for i := minPage; i <= serverMaxPage; i++ {
		wg.Add(1)
		go func(i int) {
			c <- i
			setData(strconv.Itoa(i))
			wg.Done()
			<-c
		}(i)
	}

	wg.Wait()
	close(c)
}

func setData(page string) {

	hel.Pl("Getting page", page)

	doc, err := goquery.NewDocument(url + page)

	hel.PlP("error having newDocument", err)

	table := doc.Find("table")
	tbody := table.Find("tbody")

	tbody.Find("tr").Each(func(i int, tr *goquery.Selection) {
		// var maps = map[string]string{}

		var data xData
		var hadDataFault = false

		tr.Find("td").Each(func(j int, td *goquery.Selection) {
			var val string
			// comment
			if j == 13 {
				val = strings.TrimSpace(td.Text())
			} else {
				val = bnToEn(td.Text())
				if len(val) == 0 {
					val = "0"
				}
				if j != 0 && j != 1 {
					_, err := strconv.ParseFloat(val, 64)
					if err != nil {
						hel.Pl("Error at dataFloat, j =", j, "and page", page)
						panic("ENd of ")
					}
				}
			}

			if j == 0 {
				y, m, d, err := dateReverse(val, page)
				if err != nil {
					hadDataFault = true
				}
				data.Year = y
				data.Month = m
				data.Day = d
			} else if j == 1 {
				data.Time = val
			} else if j == 2 {
				data.Produced = val
			} else if j == 3 {
				data.Load = val
			} else if j == 4 {
				data.Less = val
			} else if j == 5 {
				data.LoadShed = val
			} else if j == 6 {
				data.Gas = val
			} else if j == 7 {
				data.Liquid = val
			} else if j == 8 {
				data.Coal = val
			} else if j == 9 {
				data.Hydro = val
			} else if j == 10 {
				data.Solar = val
			} else if j == 11 {
				data.Veramara = val
			} else if j == 12 {
				data.Tripura = val
			} else if j == 13 {
				data.Comment = val
			}
		})

		// datas = append(datas, data)
		if hadDataFault == false {
			db.Create(&data)
		}

	})

}

// 12:00:00 => 12:00
func timeFormat(t string) string {
	return t[0:5]
}

// date = 31-10-2019
// 31-10-2019 => 2019-10-31
// 31 - 10 - 2019
// 01 2 34 5 6789
func dateReverse(date string, page string) (year int, month int, day int, err error) {
	if len(date) != 10 {
		hel.Pl("Wrong Date at page", page)
		err = errors.New("Wrong date")
		return
	}
	year, err = strconv.Atoi(date[6:])
	month, err = strconv.Atoi(date[3:5])
	day, err = strconv.Atoi(date[0:2])
	return
}

func timify(v int) string {
	s := strconv.Itoa(v)
	if len(s) == 1 {
		return "0" + s
	}
	return s
}

// input str ex => ১৮-০৮-২০২০
func bnToEn(str string) string {
	var o = ""
	for _, r := range str {
		c := string(r)
		var foundIndex = -1
		for j := range numbers {
			if numbers[j] == c {
				foundIndex = j
				break
			}
		}
		if foundIndex == -1 {
			o += c
		} else {
			o += strconv.Itoa(foundIndex)
		}
	}
	return o
}

func setDB() {
	var err error
	db, err = gorm.Open("sqlite3", "db.sqlite")
	hel.PlP("error in db opening", err)
	db.AutoMigrate(&xData{})
}
