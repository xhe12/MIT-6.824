package my_main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func SerialCrawl(url string, depth int, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		Crawl(u, depth-1, fetcher)
	}
	return
}

func ConncurrentCrawl(url string, depth int, fetcher Fetcher) {
	tasks := make(chan Task, 1)
	var mu sync.RWMutex
	var wg sync.WaitGroup

	fetchedDB := map[string]bool
	tasks <- Task{url, depth}
	wg.Add(1)

	go func() {
		wg.Wait
		close(tasks)
	}()

	for task := range tasks {
		go func(mu *sync.RWMutex) {
			body, children_urls, err := fetcher.Fetch(task.url)
			fmt.Printf("found: %s %q\n", task.url, body)
			mu.RLock()
			fetchedDB[task.url] = true
			mu.RUnLock()
			wg.Done()

			for _, url := range children_urls {
				// TO DO: depth >=0 and url is not in fetched
				mu.RLo
				_, exists = fetchedDB[url]
				tasks <- Task{url, d}
				wg.Add(1)
			}
		}(&mu)

	}

}

func ProcessTask(tasks <-chan Task, fetcher *Fetcher, wg *sync.WaitGroup) (<-chan Task, []string, int) {

	return tasks, children_urls, task.depth - 1

}

func main() {
	Crawl("https://golang.org/", 4, fetcher)
}

type Task struct {
	url   string
	depth int
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
