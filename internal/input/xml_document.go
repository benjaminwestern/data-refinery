package input

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
)

// XMLDocument is a mutable XML document wrapper backed by the shared input
// model. It supports record-path selection, record replacement, record
// deletion, and deterministic serialization.
type XMLDocument struct {
	sourcePath string
	root       *xmlElement
}

// LoadXMLDocument parses an XML document into the shared input tree model.
func LoadXMLDocument(sourcePath string, reader io.Reader) (*XMLDocument, error) {
	root, err := parseXMLDocument(reader)
	if err != nil {
		return nil, fmt.Errorf("parse XML input in %s: %w", sourcePath, err)
	}
	return &XMLDocument{
		sourcePath: sourcePath,
		root:       root,
	}, nil
}

// Records returns the logical records selected by the provided XML record
// path. When recordPath is empty, the whole document root is returned as one
// record.
func (d *XMLDocument) Records(recordPath string) ([]Record, error) {
	records, _, err := buildXMLRecords(d.sourcePath, d.root, recordPath)
	return records, err
}

// ReplaceRecord replaces one matched XML record with the provided normalized
// record data while preserving the surrounding document structure.
func (d *XMLDocument) ReplaceRecord(recordPath string, index int, data map[string]any) error {
	matches, err := d.matchingElements(recordPath)
	if err != nil {
		return err
	}
	if index < 0 || index >= len(matches) {
		return fmt.Errorf("XML record index %d out of range for %s", index, d.sourcePath)
	}
	if data == nil {
		data = map[string]any{}
	}
	return applyNormalizedMapToElement(matches[index], data)
}

// DeleteRecord deletes one matched XML record from the document. Deleting the
// document root is not allowed because it would leave the XML payload invalid.
func (d *XMLDocument) DeleteRecord(recordPath string, index int) error {
	matches, err := d.matchingElements(recordPath)
	if err != nil {
		return err
	}
	if index < 0 || index >= len(matches) {
		return fmt.Errorf("XML record index %d out of range for %s", index, d.sourcePath)
	}

	target := matches[index]
	if target == nil {
		return fmt.Errorf("XML record index %d is nil in %s", index, d.sourcePath)
	}
	if target.parent == nil {
		return fmt.Errorf("cannot delete the XML document root in %s", d.sourcePath)
	}

	removeXMLChild(target.parent, target)
	return nil
}

// Marshal serializes the current XML document into deterministic indented XML.
func (d *XMLDocument) Marshal() ([]byte, error) {
	if d == nil || d.root == nil {
		return nil, fmt.Errorf("XML document is empty")
	}

	var buf bytes.Buffer
	encoder := xml.NewEncoder(&buf)
	encoder.Indent("", "  ")
	if err := encodeXMLElement(encoder, d.root); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, fmt.Errorf("flush XML encoder for %s: %w", d.sourcePath, err)
	}
	if buf.Len() == 0 || buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func (d *XMLDocument) matchingElements(recordPath string) ([]*xmlElement, error) {
	if d == nil || d.root == nil {
		return nil, fmt.Errorf("empty XML document in %s", d.sourcePath)
	}

	recordPath = strings.TrimSpace(recordPath)
	if recordPath == "" {
		return []*xmlElement{d.root}, nil
	}

	segments, err := parseXMLRecordPath(recordPath, d.root.name)
	if err != nil {
		return nil, fmt.Errorf("invalid XML record path for %s: %w", d.sourcePath, err)
	}

	elements := findXMLElementsByPath(d.root, segments)
	if len(elements) == 0 {
		return nil, fmt.Errorf("XML record path %q did not match any elements in %s", recordPath, d.sourcePath)
	}
	return elements, nil
}

func applyNormalizedMapToElement(elem *xmlElement, data map[string]any) error {
	if elem == nil {
		return fmt.Errorf("XML element cannot be nil")
	}

	elem.attrs = rebuildXMLAttrs(elem.attrs, data)

	if content, ok := data["#content"]; ok {
		children, rebuiltContent, err := buildXMLMixedContent(content)
		if err != nil {
			return err
		}
		elem.children = children
		elem.content = rebuiltContent
		for _, child := range elem.children {
			child.parent = elem
		}
		return nil
	}

	desiredChildren := normalizedXMLChildValues(data)
	children, err := rebuildXMLChildren(elem.children, desiredChildren)
	if err != nil {
		return err
	}

	for _, child := range children {
		child.parent = elem
	}
	elem.children = children

	text := ""
	if value, ok := data["#text"]; ok {
		text = strings.TrimSpace(fmt.Sprint(value))
	}

	switch {
	case len(children) == 0 && text != "":
		elem.content = []any{text}
	case len(children) == 0:
		elem.content = nil
	default:
		elem.content = make([]any, 0, len(children))
		for _, child := range children {
			elem.content = append(elem.content, child)
		}
	}

	return nil
}

func rebuildXMLAttrs(existing []xml.Attr, data map[string]any) []xml.Attr {
	desired := make(map[string]string)
	for key, value := range data {
		if !strings.HasPrefix(key, "@") {
			continue
		}
		name := xmlLocalName(strings.TrimPrefix(key, "@"))
		if name == "" {
			continue
		}
		desired[name] = fmt.Sprint(value)
	}

	attrs := make([]xml.Attr, 0, len(desired))
	seen := make(map[string]bool, len(existing))
	for _, attr := range existing {
		name := xmlLocalName(attr.Name.Local)
		if name == "" {
			continue
		}
		value, ok := desired[name]
		if !ok {
			continue
		}
		attrs = append(attrs, xml.Attr{Name: xml.Name{Local: name}, Value: value})
		seen[name] = true
	}

	remaining := make([]string, 0, len(desired))
	for name := range desired {
		if !seen[name] {
			remaining = append(remaining, name)
		}
	}
	sort.Strings(remaining)
	for _, name := range remaining {
		attrs = append(attrs, xml.Attr{Name: xml.Name{Local: name}, Value: desired[name]})
	}

	return attrs
}

func normalizedXMLChildValues(data map[string]any) map[string][]any {
	children := make(map[string][]any)
	for key, value := range data {
		if key == "" || strings.HasPrefix(key, "@") || key == "#text" || key == "#content" {
			continue
		}

		name := xmlLocalName(key)
		if name == "" {
			continue
		}

		if values, ok := value.([]any); ok {
			children[name] = append(children[name], values...)
			continue
		}
		children[name] = append(children[name], value)
	}
	return children
}

func rebuildXMLChildren(existing []*xmlElement, desired map[string][]any) ([]*xmlElement, error) {
	remaining := make(map[string][]any, len(desired))
	for name, values := range desired {
		copied := make([]any, len(values))
		copy(copied, values)
		remaining[name] = copied
	}

	rebuilt := make([]*xmlElement, 0, len(existing))
	for _, child := range existing {
		if child == nil {
			continue
		}
		values := remaining[child.name]
		if len(values) == 0 {
			continue
		}
		value := values[0]
		remaining[child.name] = values[1:]
		if err := applyNormalizedValueToElement(child, value); err != nil {
			return nil, err
		}
		rebuilt = append(rebuilt, child)
	}

	leftoverNames := make([]string, 0, len(remaining))
	for name, values := range remaining {
		if len(values) > 0 {
			leftoverNames = append(leftoverNames, name)
		}
	}
	sort.Strings(leftoverNames)

	for _, name := range leftoverNames {
		for _, value := range remaining[name] {
			child, err := newXMLElementFromNormalized(name, value)
			if err != nil {
				return nil, err
			}
			rebuilt = append(rebuilt, child)
		}
	}

	return rebuilt, nil
}

func buildXMLMixedContent(value any) ([]*xmlElement, []any, error) {
	contentItems, ok := value.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("XML #content must be an array, got %T", value)
	}

	children := make([]*xmlElement, 0, len(contentItems))
	content := make([]any, 0, len(contentItems))
	for _, item := range contentItems {
		switch typed := item.(type) {
		case string:
			text := strings.TrimSpace(typed)
			if text == "" {
				continue
			}
			content = append(content, text)
		case map[string]any:
			if len(typed) != 1 {
				return nil, nil, fmt.Errorf("XML mixed-content child entries must contain exactly one element, got %d", len(typed))
			}
			for name, childValue := range typed {
				child, err := newXMLElementFromNormalized(name, childValue)
				if err != nil {
					return nil, nil, err
				}
				children = append(children, child)
				content = append(content, child)
			}
		default:
			text := strings.TrimSpace(fmt.Sprint(typed))
			if text == "" {
				continue
			}
			content = append(content, text)
		}
	}

	return children, content, nil
}

func applyNormalizedValueToElement(elem *xmlElement, value any) error {
	if elem == nil {
		return fmt.Errorf("XML element cannot be nil")
	}

	switch typed := value.(type) {
	case map[string]any:
		return applyNormalizedMapToElement(elem, typed)
	case nil:
		elem.attrs = nil
		elem.children = nil
		elem.content = nil
		return nil
	default:
		elem.attrs = nil
		elem.children = nil
		text := strings.TrimSpace(fmt.Sprint(typed))
		if text == "" {
			elem.content = nil
			return nil
		}
		elem.content = []any{text}
		return nil
	}
}

func newXMLElementFromNormalized(name string, value any) (*xmlElement, error) {
	elem := &xmlElement{
		name: xmlLocalName(name),
	}
	if elem.name == "" {
		return nil, fmt.Errorf("XML element name cannot be empty")
	}
	if err := applyNormalizedValueToElement(elem, value); err != nil {
		return nil, err
	}
	return elem, nil
}

func removeXMLChild(parent, child *xmlElement) {
	if parent == nil || child == nil {
		return
	}

	rebuiltChildren := make([]*xmlElement, 0, len(parent.children))
	for _, existing := range parent.children {
		if existing == child {
			continue
		}
		rebuiltChildren = append(rebuiltChildren, existing)
	}
	parent.children = rebuiltChildren

	rebuiltContent := make([]any, 0, len(parent.content))
	for _, item := range parent.content {
		existingChild, ok := item.(*xmlElement)
		if ok && existingChild == child {
			continue
		}
		rebuiltContent = append(rebuiltContent, item)
	}
	parent.content = rebuiltContent
}

func encodeXMLElement(encoder *xml.Encoder, elem *xmlElement) error {
	if elem == nil {
		return nil
	}

	start := xml.StartElement{
		Name: xml.Name{Local: elem.name},
		Attr: append([]xml.Attr(nil), elem.attrs...),
	}
	if err := encoder.EncodeToken(start); err != nil {
		return fmt.Errorf("encode start element %q: %w", elem.name, err)
	}

	content := elem.content
	if len(content) == 0 && len(elem.children) > 0 {
		content = make([]any, 0, len(elem.children))
		for _, child := range elem.children {
			content = append(content, child)
		}
	}

	for _, item := range content {
		switch typed := item.(type) {
		case string:
			if err := encoder.EncodeToken(xml.CharData([]byte(typed))); err != nil {
				return fmt.Errorf("encode XML text for %q: %w", elem.name, err)
			}
		case *xmlElement:
			if err := encodeXMLElement(encoder, typed); err != nil {
				return err
			}
		}
	}

	if err := encoder.EncodeToken(start.End()); err != nil {
		return fmt.Errorf("encode end element %q: %w", elem.name, err)
	}
	return nil
}
