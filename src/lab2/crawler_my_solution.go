package main

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
		SerialCrawl(u, depth-1, fetcher)
	}
	return
}

func ConncurrentCrawl(url string, depth int, fetcher Fetcher) {
	tasks := make(chan Task, 1)
	var mu sync.RWMutex
	var wg sync.WaitGroup

	fetchedDB := make(map[string]bool)
	done := false
	tasks <- Task{url, depth}
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(tasks)
		mu.Lock()
		done = true
		mu.Unlock()
	}(&wg)

	for {
		mu.RLock()
		if done {
			return
		}
		mu.RUnlock()
		task := <-tasks
		go func() {
			defer wg.Done()
			fmt.Printf("Processing task %v\n", task)
			body, children_urls, err := fetcher.Fetch(task.url)
			if err != nil {
				mu.Lock()
				fetchedDB[task.url] = true
				mu.Unlock()
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("found: %s %q\n", task.url, body)
				mu.Lock()
				fetchedDB[task.url] = true
				mu.Unlock()

				for _, url := range children_urls {
					mu.RLock()
					_, exists := fetchedDB[url]
					mu.RUnlock()
					if !exists && task.depth > 0 {
						t := Task{url, task.depth - 1}
						wg.Add(1)
						fmt.Printf("Adding task %v\n", t)
						tasks <- t
					}

				}
			}

		}()
	}

}

func main() {
	fmt.Println("***Starting serial crawler...")
	SerialCrawl("https://golang.org/", 4, fetcher)
	fmt.Println("***Starting concurrent crawler...")
	ConncurrentCrawl("https://golang.org/", 4, fetcher)
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
