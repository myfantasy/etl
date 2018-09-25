package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/denisenkom/go-mssqldb"

	"github.com/myfantasy/mfdb"
	"github.com/myfantasy/mfe"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
)

// LoadAndInsertFromQuery - is try select and insert query
func LoadAndInsertFromQuery(dbTypeFrom, dbFromCS, dbTypeTo, dbToCS, queryFrom, tableTo string, verbose bool) (e error) {
	if verbose {
		fmt.Println("LoadAndInsertFromQuery start...")
	}
	if verbose {
		fmt.Println("Open source...")
	}
	dbFrom, err := sql.Open(dbTypeFrom, dbFromCS)
	if err != nil {
		return err
	}
	if verbose {
		fmt.Println("Open destination...")
	}
	dbTo, err := sql.Open(dbTypeTo, dbToCS)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Println("Load data from source...")
	}

	err = mfdb.ExecuteWithPrepareBatch(context.Background(), dbFrom, queryFrom, "", 10000, func(v mfe.Variant) (err error) {
		var eri error
		if dbTypeTo == "sqlserver" {
			fmt.Println("ms...")
			//err = InsertDBTableMS(context.Background(), &vs, time.Second*300, "", tableTo, dbTo)
			eri = CopyMS(dbTo, tableTo, v, verbose)
		} else {
			fmt.Println("...")
			eri = InsertDBTable(context.Background(), &v, time.Second*300, "", tableTo, dbTo)
		}
		return eri
	})
	if err != nil {
		return err
	}
	return

	//v, err := mfdb.Execute(dbFrom, queryFrom)
	v, err := LoadDB(context.Background(), time.Second*300, "", queryFrom, dbFrom)
	if err != nil {
		return err
	}

	if verbose {
		fmt.Println("Write data to destination...")
	}
	for i, vs := range v.GI(0).SplitBy(10000) {
		if verbose {
			fmt.Println("write index " + fmt.Sprint(i))
		}
		if dbTypeTo == "sqlserver" {
			fmt.Println("ms...")
			//err = InsertDBTableMS(context.Background(), &vs, time.Second*300, "", tableTo, dbTo)
			CopyMS(dbTo, tableTo, vs, verbose)
		} else {
			fmt.Println("...")
			err = InsertDBTable(context.Background(), &vs, time.Second*300, "", tableTo, dbTo)
		}
		if err != nil {
			return err
		}
	}

	if verbose {
		fmt.Println("LoadAndInsertFromQuery Complete.")
	}
	return err

}

// LoadDB load data from connection by query
func LoadDB(ctx context.Context, timeout time.Duration, prepareQuery string, query string, db *sql.DB) (v mfe.Variant, e error) {

	// if timeout > time.Millisecond {
	// 	var cancel context.CancelFunc
	// 	ctx, cancel = context.WithTimeout(ctx, timeout)
	// 	defer cancel()
	// }
	v, e = mfdb.ExecuteWithPrepare(ctx, db, query, prepareQuery)
	return
}

// LoadDBTable load all data from connection by table name
func LoadDBTable(ctx context.Context, timeout time.Duration, prepareQuery string, table string, db *sql.DB) (v mfe.Variant, e error) {

	v, e = LoadDB(ctx, timeout, prepareQuery, "select * from "+table, db)
	return
}

// InsertDBTable insert values as simple insert from variant
func InsertDBTable(ctx context.Context, v *mfe.Variant, timeout time.Duration, prepareQuery string, table string, db *sql.DB) (e error) {
	query := mfdb.InsertQuery(v, table)
	_, err := mfdb.ExecuteWithPrepare(ctx, db, query, prepareQuery)
	return err
}

// InsertDBTableMS insert values as simple insert from variant into ms db
func InsertDBTableMS(ctx context.Context, v *mfe.Variant, timeout time.Duration, prepareQuery string, table string, db *sql.DB) (e error) {
	query := mfdb.InsertQueryMS(v, table)
	_, err := mfdb.ExecuteWithPrepare(ctx, db, query, prepareQuery)
	return err
}

func CopyMS(db *sql.DB, tableName string, v mfe.Variant, verbose bool) (err error) {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	fields := v.GI(0).Keys()

	if verbose {
		fmt.Println(fields)
	}

	stmt, err := txn.Prepare(mssql.CopyIn(tableName, mssql.BulkOptions{}, fields...))
	if err != nil {
		return err
	}
	if verbose {
		fmt.Println("prep1")
	}
	for _, vi := range v.SV() {
		var ins []interface{}
		for _, n := range fields {

			ins = append(ins, int(vi.GE(n).Dec().IntPart()))

		}

		_, err = stmt.Exec(ins...)
		if err != nil {
			return err
		}
	}
	if verbose {
		fmt.Println("Prepared")
	}

	result, err := stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	if verbose {
		rowCount, _ := result.RowsAffected()
		fmt.Printf("%d row copied\n", rowCount)

	}

	return nil
}
