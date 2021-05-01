package main

import (
 abcitypes "github.com/tendermint/tendermint/abci/types"
 "github.com/tendermint/tmlibs/merkle"
 "database/sql"
 _ "bytes"
 "fmt"
 parser "github.com/xwb1989/sqlparser"
 "golang.org/x/crypto/ripemd160"
 "encoding/json"
 "strings"
 "strconv"
)

type Statement []byte

func (st Statement) Hash() []byte {
  hasher := ripemd160.New()
  hasher.Write(st)
  return hasher.Sum(nil)
}

type PGMint struct {
 db           *sql.DB
 lastBlock    int64
 lastHash     []byte
 entries      []merkle.Hasher
}

func NewPGMint(db *sql.DB) *PGMint {
 return &PGMint{
  db: db,
  lastBlock: 0,
  lastHash:  []byte(""),
 }
}

var _ abcitypes.Application = (*PGMint)(nil)

func isValid(val string) (int, error) {
  stmt, err := parser.Parse(val)
  if err != nil {
    return 0, fmt.Errorf("SQL Error in %s", val)
  }
  switch cmdType := stmt.(type) {
    case *parser.DDL:
      if cmdType.Action == "drop" {
        return 0, fmt.Errorf("DROP statements are not allowed")
      }
    case *parser.Select:
      return 1, nil
  }
  return 2, nil
}

func (app *PGMint) Info(req abcitypes.RequestInfo) abcitypes.ResponseInfo {
 return abcitypes.ResponseInfo{
    Data:             "pgmint",
    Version:          "1",
    LastBlockHeight:  app.lastBlock,
    LastBlockAppHash: app.lastHash,
  }
}

func (app *PGMint) DeliverTx(req abcitypes.RequestDeliverTx) abcitypes.ResponseDeliverTx {
  val := strings.Replace(string(req.Tx), "\\\"", "\"", -1)

  _, err := app.db.Exec(val)
  if err != nil {
    return abcitypes.ResponseDeliverTx{Code: 1, Log: err.Error() + " -- " + string(req.Tx)} // deliver failed
  }

  app.entries = append(app.entries, Statement(req.Tx))

  words := strings.Fields(string(req.Tx))
  stmt_type := strings.ToUpper(words[0])

  return abcitypes.ResponseDeliverTx{Code: abcitypes.CodeTypeOK, Log: stmt_type}
}

func (app *PGMint) CheckTx(req abcitypes.RequestCheckTx) abcitypes.ResponseCheckTx {
 val := strings.Replace(string(req.Tx), "\\\"", "\"", -1)

 code, err := isValid(val)
 if err != nil {
    return abcitypes.ResponseCheckTx{Code: 1, Log: err.Error()}
 } 

 if code == 1 {
    return abcitypes.ResponseCheckTx{Code: 1, Log: "Wrong path for Read"}
 }

 return abcitypes.ResponseCheckTx{Code: abcitypes.CodeTypeOK}
}


func (app *PGMint) Commit() abcitypes.ResponseCommit {
  if len(app.entries) > 0 {
    app.lastHash = merkle.SimpleHashFromHashers(app.entries)
  }
  app.entries = nil
  return abcitypes.ResponseCommit{Data: app.lastHash}
}



func (app *PGMint) Query(req abcitypes.RequestQuery) (res abcitypes.ResponseQuery) {
  
  code, err := isValid(string(req.Data)); 

  if err != nil {
    panic(err)
  }
  if code == 2 {
    panic(err)
  }

  rows, err := app.db.Query(string(req.Data))
  defer func() {
    if rows != nil {
      rows.Close()
    }
  }()
  if err != nil {
    panic(err)
  }


  output := make(map[string][]interface{})

  s := make(map[string]struct{})
  s_ind := make(map[int]struct{})
  var exists = struct{}{}

  colNames, err := rows.Columns()
  if err != nil {
    panic(err)
  }

  // Fetch all values, make splits
  values := make([]interface{}, len(colNames))
  scanArgs := make([]interface{}, len(values))

  for i := range values {
    scanArgs[i] = &values[i]

    _, ex := s[colNames[i]]
    if ex == true {
      continue
    }
    s[colNames[i]] = exists
    s_ind[i] = exists
  }

  for rows.Next() {
    err = rows.Scan(scanArgs...)
    if err != nil {
      panic(err)
    }

    for i, value := range values {
      // exclude columns that occur multiple times due to a join
      _, ex := s_ind[i]
      if ex == false {
        continue
      }

      switch value.(type) {
        case nil:
          output[colNames[i]] = append(output[colNames[i]], "NULL")
        case []byte:
          output[colNames[i]] = append(output[colNames[i]] , string(value.([]byte)))
        default:
          output[colNames[i]]  = append(output[colNames[i]], value)
      }
    }
  }

  tmp, err := json.Marshal(output)
  if err != nil {
    panic(err)
  }
  res.Info = string(tmp)

  res.Key = req.Data
  res.Value = []byte(strconv.Itoa(len(values)))

  return
}

func (app *PGMint) InitChain(req abcitypes.RequestInitChain) abcitypes.ResponseInitChain {
 return abcitypes.ResponseInitChain{}
}

func (app *PGMint) BeginBlock(req abcitypes.RequestBeginBlock) abcitypes.ResponseBeginBlock {
 app.entries = make([]merkle.Hasher, 0)
 return abcitypes.ResponseBeginBlock{}
}


func (app *PGMint) EndBlock(req abcitypes.RequestEndBlock) abcitypes.ResponseEndBlock {
 app.lastBlock = req.GetHeight()
 return abcitypes.ResponseEndBlock{}
}

func (app *PGMint) ListSnapshots(abcitypes.RequestListSnapshots) abcitypes.ResponseListSnapshots {
 return abcitypes.ResponseListSnapshots{}
}

func (app *PGMint) OfferSnapshot(abcitypes.RequestOfferSnapshot) abcitypes.ResponseOfferSnapshot {
 return abcitypes.ResponseOfferSnapshot{}
}

func (app *PGMint) LoadSnapshotChunk(abcitypes.RequestLoadSnapshotChunk) abcitypes.ResponseLoadSnapshotChunk {
 return abcitypes.ResponseLoadSnapshotChunk{}
}

func (app *PGMint) ApplySnapshotChunk(abcitypes.RequestApplySnapshotChunk) abcitypes.ResponseApplySnapshotChunk {
 return abcitypes.ResponseApplySnapshotChunk{}
}