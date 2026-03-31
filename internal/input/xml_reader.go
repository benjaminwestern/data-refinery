package input

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
)

type xmlReader struct {
	sourcePath string
	reader     io.ReadCloser
	layout     Layout
	records    []Record
	nextIndex  int
}

type xmlElement struct {
	name      string
	attrs     []xml.Attr
	parent    *xmlElement
	children  []*xmlElement
	content   []any
	startLine int
}

func newXMLReader(sourcePath string, reader io.ReadCloser, recordPath string) (Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	root, err := parseXMLDocument(reader)
	if err != nil {
		safeClose(reader)
		return nil, fmt.Errorf("parse XML input in %s: %w", sourcePath, err)
	}

	records, layout, err := buildXMLRecords(sourcePath, root, recordPath)
	if err != nil {
		safeClose(reader)
		return nil, err
	}

	return &xmlReader{
		sourcePath: sourcePath,
		reader:     reader,
		layout:     layout,
		records:    records,
	}, nil
}

func (r *xmlReader) Format() Format {
	return FormatXML
}

func (r *xmlReader) Layout() Layout {
	return r.layout
}

func (r *xmlReader) Next(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, fmt.Errorf("read XML input %s: %w", r.sourcePath, err)
	}
	if r.nextIndex >= len(r.records) {
		return Record{}, io.EOF
	}

	record := r.records[r.nextIndex]
	r.nextIndex++
	return record, nil
}

func (r *xmlReader) Close() error {
	return closeAndNil(&r.reader)
}

func parseXMLDocument(reader io.Reader) (*xmlElement, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read XML input: %w", err)
	}

	decoder := xml.NewDecoder(bytes.NewReader(data))
	newlineOffsets := xmlNewlineOffsets(data)

	var (
		stack []*xmlElement
		root  *xmlElement
	)

	for {
		startOffset := decoder.InputOffset()
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read XML token: %w", err)
		}

		switch typed := token.(type) {
		case xml.StartElement:
			elem := &xmlElement{
				name:      xmlLocalName(typed.Name.Local),
				attrs:     append([]xml.Attr(nil), typed.Attr...),
				startLine: xmlLineNumberForOffset(newlineOffsets, startOffset),
			}
			stack = append(stack, elem)

		case xml.CharData:
			if len(stack) == 0 {
				continue
			}
			text := strings.TrimSpace(string(typed))
			if text == "" {
				continue
			}
			current := stack[len(stack)-1]
			current.content = append(current.content, text)

		case xml.EndElement:
			if len(stack) == 0 {
				return nil, fmt.Errorf("unexpected closing tag %q", xmlLocalName(typed.Name.Local))
			}

			elem := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if len(stack) == 0 {
				if root != nil {
					return nil, fmt.Errorf("multiple root elements encountered")
				}
				root = elem
				continue
			}

			parent := stack[len(stack)-1]
			elem.parent = parent
			parent.children = append(parent.children, elem)
			parent.content = append(parent.content, elem)
		}
	}

	if len(stack) != 0 {
		return nil, fmt.Errorf("unterminated XML document")
	}
	if root == nil {
		return nil, fmt.Errorf("empty XML document")
	}

	return root, nil
}

func normalizeXMLRoot(root *xmlElement) map[string]any {
	if root == nil {
		return map[string]any{}
	}

	normalized := normalizeXMLElement(root)
	if obj, ok := normalized.(map[string]any); ok {
		return obj
	}

	return map[string]any{
		"#text": normalized,
	}
}

func buildXMLRecords(sourcePath string, root *xmlElement, recordPath string) ([]Record, Layout, error) {
	if root == nil {
		return nil, LayoutUnknown, fmt.Errorf("empty XML document in %s", sourcePath)
	}

	recordPath = strings.TrimSpace(recordPath)
	if recordPath == "" {
		return []Record{
			{
				SourcePath:  sourcePath,
				RecordIndex: 0,
				LineNumber:  xmlRecordLineNumber(root),
				Data:        normalizeXMLRoot(root),
			},
		}, LayoutObject, nil
	}

	segments, err := parseXMLRecordPath(recordPath, root.name)
	if err != nil {
		return nil, LayoutUnknown, fmt.Errorf("invalid XML record path for %s: %w", sourcePath, err)
	}

	elements := findXMLElementsByPath(root, segments)
	if len(elements) == 0 {
		return nil, LayoutUnknown, fmt.Errorf("XML record path %q did not match any elements in %s", recordPath, sourcePath)
	}

	records := make([]Record, 0, len(elements))
	for index, elem := range elements {
		records = append(records, Record{
			SourcePath:  sourcePath,
			RecordIndex: index,
			LineNumber:  xmlRecordLineNumber(elem),
			Data:        normalizeXMLRoot(elem),
		})
	}

	return records, LayoutStream, nil
}

func parseXMLRecordPath(recordPath, rootName string) ([]string, error) {
	if strings.TrimSpace(recordPath) == "" {
		return nil, fmt.Errorf("record path cannot be empty")
	}

	rawSegments := strings.Split(recordPath, ".")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, fmt.Errorf("record path %q contains an empty segment", recordPath)
		}
		segments = append(segments, xmlLocalName(segment))
	}

	if len(segments) > 0 && segments[0] == xmlLocalName(rootName) {
		segments = segments[1:]
	}

	return segments, nil
}

func findXMLElementsByPath(root *xmlElement, path []string) []*xmlElement {
	if root == nil {
		return nil
	}
	if len(path) == 0 {
		return []*xmlElement{root}
	}

	var matches []*xmlElement
	for _, child := range root.children {
		if child == nil || child.name != path[0] {
			continue
		}
		if len(path) == 1 {
			matches = append(matches, child)
			continue
		}
		matches = append(matches, findXMLElementsByPath(child, path[1:])...)
	}
	return matches
}

func xmlNewlineOffsets(data []byte) []int {
	offsets := make([]int, 0, bytes.Count(data, []byte{'\n'}))
	for index, b := range data {
		if b == '\n' {
			offsets = append(offsets, index)
		}
	}
	return offsets
}

func xmlLineNumberForOffset(newlineOffsets []int, offset int64) int {
	line := sort.Search(len(newlineOffsets), func(i int) bool {
		return newlineOffsets[i] >= int(offset)
	}) + 1
	if line < 1 {
		return 1
	}
	return line
}

func xmlRecordLineNumber(elem *xmlElement) int {
	if elem == nil || elem.startLine <= 0 {
		return 1
	}
	return elem.startLine
}

func xmlLocalName(name string) string {
	name = strings.TrimSpace(name)
	if idx := strings.LastIndex(name, ":"); idx >= 0 {
		name = name[idx+1:]
	}
	return name
}

func normalizeXMLElement(elem *xmlElement) any {
	if elem == nil {
		return nil
	}

	childrenByName := make(map[string][]any)
	for _, child := range elem.children {
		if child == nil {
			continue
		}
		childrenByName[child.name] = append(childrenByName[child.name], normalizeXMLElement(child))
	}

	textParts := make([]string, 0, len(elem.content))
	content := make([]any, 0, len(elem.content))
	hasMixedContent := false

	for _, item := range elem.content {
		switch typed := item.(type) {
		case string:
			if typed == "" {
				continue
			}
			textParts = append(textParts, typed)
			content = append(content, typed)
			if len(elem.children) > 0 {
				hasMixedContent = true
			}
		case *xmlElement:
			hasMixedContent = hasMixedContent || len(textParts) > 0
			content = append(content, map[string]any{
				typed.name: normalizeXMLElement(typed),
			})
		}
	}

	text := strings.TrimSpace(strings.Join(textParts, " "))

	if len(elem.attrs) == 0 && len(childrenByName) == 0 {
		if text == "" {
			return ""
		}
		return text
	}

	obj := make(map[string]any)
	for _, attr := range elem.attrs {
		name := xmlLocalName(attr.Name.Local)
		if name == "" {
			continue
		}
		obj["@"+name] = attr.Value
	}

	for name, values := range childrenByName {
		if len(values) == 1 {
			obj[name] = values[0]
			continue
		}
		obj[name] = values
	}

	if text != "" {
		obj["#text"] = text
	}

	if hasMixedContent {
		obj["#content"] = content
	}

	return obj
}
