package dmlbuilder

import (
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"reflect"
	"strconv"
	"strings"
)

const (
	QUOTE = "'"
	NULL  = "NULL"
)

func GetDML(event *canal.RowsEvent) string {
	var table = event.Table.Name
	var schema = event.Table.Schema
	var values string
	switch event.Action {
	case canal.InsertAction:
		values, _ = parseValues(event.Rows[0])
	case canal.UpdateAction:
		values, _ = parseValues(event.Rows[1])
	case canal.DeleteAction:
		return deleteDML(event)
	}
	return fmt.Sprintf("REPLACE INTO %s.%s VALUES (%s);", schema, table, values)
}

func parseValues(rows []interface{}) (string, error) {
	var values = make([]string, len(rows))
	var err error
	for i, c := range rows {
		if values[i], err = typeToString(c); err != nil {
			return "", err
		}
	}
	return strings.Join(values, ","), nil
}

func deleteDML(event *canal.RowsEvent) string {
	if len(event.Table.PKColumns) > 0 {
		return deleteWithPK(event)
	}
	return deleteFullRow(event)
}

func deleteFullRow(event *canal.RowsEvent) string {
	values := make([]string, len(event.Table.Columns))
	for i, c := range event.Table.Columns {
		val, _ := typeToString(event.Rows[0][i])
		values[i] = c.Name + "=" + val
	}
	where := strings.Join(values, " AND ")
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s", event.Table.Schema, event.Table.Name, where)
}

func deleteWithPK(event *canal.RowsEvent) string {
	values := make([]string, len(event.Table.PKColumns))
	for _, i := range event.Table.PKColumns {
		colname := event.Table.Columns[i].Name
		val, _ := typeToString(event.Rows[0][i])
		values[i] = colname + "=" + val
	}
	whereclause := strings.Join(values, " AND ")
	return fmt.Sprintf("DELETE FROM %s.%s WHERE %s", event.Table.Schema, event.Table.Name, whereclause)
}

func typeToString(c interface{}) (string, error) {
	var out string
	switch c.(type) {
	case int, int8, int16, int32, int64:
		/* 
		 * Implicitly handles BIT, BOOLEAN types
		 * ENUM and Set are stored a bitmap and returned as interger in binlog 
		  */
		value := reflect.ValueOf(c).Int()
		out = strconv.FormatInt(value, 10)
	case uint, uint8, uint16, uint32, uint64:
		value := reflect.ValueOf(c).Uint()
		out = strconv.FormatUint(value, 10)
	case float64:
		value := c.(float64)
		out = QUOTE + strconv.FormatFloat(value, 'e', -1, 64) + QUOTE
	case float32:
		value := c.(float64)
		out = QUOTE + strconv.FormatFloat(value, 'e', -1, 32) + QUOTE
	case string:
		out = QUOTE + c.(string) + QUOTE
	case []byte:
		out = QUOTE + string(c.([]byte)) + QUOTE
	case nil:
		out = NULL
	default:
		return "", fmt.Errorf("Unkown type %v", c)
	}
	return out, nil
}
