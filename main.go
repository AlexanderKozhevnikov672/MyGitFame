package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"

	"configs"
)

type FlagInfo struct {
	repository   string
	revision     string
	orderBy      string
	useCommitter bool
	format       string
	extensions   []string
	languages    []string
	exclude      []string
	restrictTo   []string
}

func CheckEntry(str string, arr []string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

func ParseFlag() (*FlagInfo, error) {
	fi := new(FlagInfo)

	var extensionsInput, languagesInput, excludeInput, restrictToInput string
	flag.StringVar(&fi.repository, "repository", ".", "repo path")
	flag.StringVar(&fi.revision, "revision", "HEAD", "commit ptr")
	flag.StringVar(&fi.orderBy, "order-by", "lines", "sort key")
	flag.BoolVar(&fi.useCommitter, "use-committer", false, "use committer")
	flag.StringVar(&fi.format, "format", "tabular", "output format")
	flag.StringVar(&extensionsInput, "extensions", "", "extensions list")
	flag.StringVar(&languagesInput, "languages", "", "languages list")
	flag.StringVar(&excludeInput, "exclude", "", "exclude list")
	flag.StringVar(&restrictToInput, "restrict-to", "", "restrict to list")
	flag.Parse()

	if !CheckEntry(fi.orderBy, []string{"lines", "commits", "files"}) {
		return nil, errors.New("unknown 'order-by' flag: " + fi.orderBy)
	}
	if !CheckEntry(fi.format, []string{"tabular", "csv", "json", "json-lines"}) {
		return nil, errors.New("unknown 'format' flag: " + fi.format)
	}
	if len(extensionsInput) > 0 {
		fi.extensions = strings.Split(extensionsInput, ",")
	}
	if len(languagesInput) > 0 {
		fi.languages = strings.Split(languagesInput, ",")
	}
	if len(excludeInput) > 0 {
		fi.exclude = strings.Split(excludeInput, ",")
	}
	if len(restrictToInput) > 0 {
		fi.restrictTo = strings.Split(restrictToInput, ",")
	}

	return fi, nil
}

type ExtensionInfo struct {
	extension map[string]bool
	language  map[string]bool
}

func ParseExtension(fi *FlagInfo) (*ExtensionInfo, error) {
	type Language struct {
		Name       string   `json:"name"`
		Type       string   `json:"type"`
		Extensions []string `json:"extensions"`
	}

	var languageData []Language
	err := json.Unmarshal(configs.JSONData, &languageData)
	if err != nil {
		return nil, err
	}

	ei := &ExtensionInfo{extension: make(map[string]bool), language: make(map[string]bool)}

	for _, e := range fi.extensions {
		ei.extension[e] = true
	}

	language := make(map[string]bool)
	for _, l := range fi.languages {
		language[l] = true
	}

	for _, l := range languageData {
		_, ok := language[strings.ToLower(l.Name)]
		if ok {
			for _, e := range l.Extensions {
				ei.language[e] = true
			}
		}
	}

	return ei, nil
}

func (ei *ExtensionInfo) CheckName(fi *FlagInfo, name string) bool {
	_, eOK := ei.extension[path.Ext(name)]
	_, lOK := ei.language[path.Ext(name)]

	return (len(fi.extensions) == 0 || eOK) && (len(fi.languages) == 0 || lOK)
}

func FindFiles(fi *FlagInfo, ei *ExtensionInfo) ([]string, error) {
	cmd := exec.Command("git", "ls-tree", "--name-only", "-r", fi.revision)
	cmd.Dir = fi.repository
	res, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	CheckName := func(name string) (bool, error) {
		if !ei.CheckName(fi, name) {
			return false, nil
		}

		for _, pattern := range fi.exclude {
			matched, err := path.Match(pattern, name)
			if err != nil {
				return false, err
			}
			if matched {
				return false, nil
			}
		}

		isFound := (len(fi.restrictTo) == 0)
		for _, pattern := range fi.restrictTo {
			matched, err := path.Match(pattern, name)
			if err != nil {
				return false, nil
			}
			if matched {
				isFound = true
				break
			}
		}

		return isFound, nil
	}

	var files []string
	names := strings.Split(string(res), "\n")
	names = names[:len(names)-1]
	for _, name := range names {
		isAllowed, err := CheckName(name)
		if err != nil {
			return nil, err
		}
		if isAllowed {
			files = append(files, name)
		}
	}

	return files, nil
}

const commitLen = 40

type CommitInfo struct {
	commit    string
	author    string
	lineCount int
}

func AnalyzeEmptyFile(fi *FlagInfo, name string) (*CommitInfo, error) {
	cmd := exec.Command("git", "log", fi.revision, "-n", "1", "--format=raw", "--", name)
	cmd.Dir = fi.repository
	res, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(res), "\n")
	i := 2
	if !strings.HasPrefix(lines[i], "author") {
		i++
	}
	author := strings.Split(lines[i], " <")[0][len("author "):]
	if fi.useCommitter {
		author = strings.Split(lines[i+1], " <")[0][len("committer "):]
	}

	return &CommitInfo{
		commit:    lines[0][len("commit "):],
		author:    author,
		lineCount: 0,
	}, nil
}

func AnalyzeFile(fi *FlagInfo, name string) (map[string]*CommitInfo, error) {
	cmd := exec.Command("git", "blame", name, "--porcelain", fi.revision)
	cmd.Dir = fi.repository
	res, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	commits := make(map[string]*CommitInfo)

	lines := strings.Split(string(res), "\n")
	lines = lines[:len(lines)-1]

	if len(lines) == 0 {
		ci, err := AnalyzeEmptyFile(fi, name)
		if err != nil {
			return nil, err
		}

		commits[ci.commit] = ci
		return commits, nil
	}

	for i := 0; i < len(lines); {
		commit := lines[i][:commitLen]

		ci, ok := commits[commit]
		if ok {
			ci.lineCount++
			i += 2
		} else {
			commits[commit] = &CommitInfo{
				commit:    commit,
				author:    lines[i+1][len("author "):],
				lineCount: 1,
			}
			if fi.useCommitter {
				commits[commit].author = lines[i+5][len("committer "):]
			}
			if !strings.HasPrefix(lines[i+10], "filename") {
				i++
			}
			i += 12
		}
	}

	return commits, nil
}

type AuthorInfo struct {
	Name    string `json:"name"`
	Commits int    `json:"commits"`
	Lines   int    `json:"lines"`
	Files   int    `json:"files"`
}

type AuthorData []*AuthorInfo

func CollectStatistics(fi *FlagInfo, files []string) (AuthorData, error) {
	fileCount := make(map[string]map[string]bool)
	commitCount := make(map[string]map[string]bool)
	lineCount := make(map[string]int)

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(files))
	doneCount := 0

	for i := range files {
		name := files[i]

		go func() {
			commits, err := AnalyzeFile(fi, name)
			if err != nil {
				panic(err)
			}

			mu.Lock()
			for _, ci := range commits {
				_, ok := fileCount[ci.author]
				if !ok {
					fileCount[ci.author] = make(map[string]bool)
				}
				fileCount[ci.author][name] = true

				_, ok = commitCount[ci.author]
				if !ok {
					commitCount[ci.author] = make(map[string]bool)
				}
				commitCount[ci.author][ci.commit] = true

				lineCount[ci.author] += ci.lineCount
			}

			doneCount++
			os.Stderr.WriteString(fmt.Sprintf("analysis done by %d percent\n", doneCount*100/len(files)))
			mu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	var authorData AuthorData
	for author := range fileCount {
		authorData = append(authorData, &AuthorInfo{
			Name:    author,
			Commits: len(commitCount[author]),
			Lines:   lineCount[author],
			Files:   len(fileCount[author]),
		})
	}

	return authorData, nil
}

func (ad AuthorData) Len() int {
	return len(ad)
}

func (ad AuthorData) Swap(i, j int) {
	ad[i], ad[j] = ad[j], ad[i]
}

var key = "lines"

func (ad AuthorData) Less(i, j int) bool {
	iVal, jVal := ad[i].Lines, ad[j].Lines
	if key == "commits" {
		iVal, jVal = ad[i].Commits, ad[j].Commits
	}
	if key == "files" {
		iVal, jVal = ad[i].Files, ad[j].Files
	}

	if iVal != jVal {
		return iVal > jVal
	}
	if ad[i].Lines != ad[j].Lines {
		return ad[i].Lines > ad[j].Lines
	}
	if ad[i].Commits != ad[j].Commits {
		return ad[i].Commits > ad[j].Commits
	}
	if ad[i].Files != ad[j].Files {
		return ad[i].Files > ad[j].Files
	}
	return ad[i].Name < ad[j].Name
}

func SortData(fi *FlagInfo, authorData AuthorData) {
	key = fi.orderBy
	sort.Sort(authorData)
}

func WriteTabular(authorData AuthorData) error {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 0, 1, ' ', 0)
	defer w.Flush()
	const format = "%v\t%v\t%v\t%v\n"

	_, err := fmt.Fprintf(w, format, "Name", "Lines", "Commits", "Files")
	if err != nil {
		return err
	}

	for _, ai := range authorData {
		_, err = fmt.Fprintf(w, format, ai.Name, ai.Lines, ai.Commits, ai.Files)
		if err != nil {
			return err
		}
	}

	return nil
}

func WriteCSV(authorData AuthorData) error {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	err := w.Write([]string{"Name", "Lines", "Commits", "Files"})
	if err != nil {
		return err
	}

	for _, ci := range authorData {
		err = w.Write([]string{
			ci.Name,
			strconv.Itoa(ci.Lines),
			strconv.Itoa(ci.Commits),
			strconv.Itoa(ci.Files),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func WriteJSON(authorData AuthorData) error {
	jsonData, err := json.Marshal(authorData)
	if err != nil {
		return err
	}

	os.Stdout.Write(jsonData)

	return nil
}

func WriteJSONLines(authorData AuthorData) error {
	for _, ci := range authorData {
		jsonData, err := json.Marshal(ci)
		if err != nil {
			return err
		}

		os.Stdout.Write(jsonData)
		os.Stdout.Write([]byte("\n"))
	}

	return nil
}

func WriteData(fi *FlagInfo, authorData AuthorData) error {
	var err error
	if fi.format == "tabular" {
		err = WriteTabular(authorData)
	} else if fi.format == "csv" {
		err = WriteCSV(authorData)
	} else if fi.format == "json" {
		err = WriteJSON(authorData)
	} else if fi.format == "json-lines" {
		err = WriteJSONLines(authorData)
	}
	return err
}

func main() {
	os.Stderr.WriteString("starting\n")

	os.Stderr.WriteString("parsing flags\n")

	fi, err := ParseFlag()
	if err != nil {
		panic(err)
	}

	os.Stderr.WriteString("parsing extensions and languages\n")

	ei, err := ParseExtension(fi)
	if err != nil {
		panic(err)
	}

	os.Stderr.WriteString("finding files\n")

	files, err := FindFiles(fi, ei)
	if err != nil {
		panic(err)
	}

	os.Stderr.WriteString("collecting statistics\n")

	authorData, err := CollectStatistics(fi, files)
	if err != nil {
		panic(err)
	}

	os.Stderr.WriteString("sorting data\n")

	SortData(fi, authorData)

	os.Stderr.WriteString("writing data\n")

	err = WriteData(fi, authorData)
	if err != nil {
		panic(err)
	}

	os.Stderr.WriteString("done\n")
}
