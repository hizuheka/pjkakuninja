package main

import (
  "os"

  "github.com/urfave/cli/v2"
)

type File struct {
	path string
	size string
}

func main() {
	var dbname string
	var sqlF string
	var sql string
	var input string
	var output string

	app := &cli.App{
		Name: "MuscleRdbunlsql",
		Version: Version,
		Usage: "力技でrdbunlsql",
		Flags: []cli.Flag {
			&cli.StringFlag{
				Name: "dbname",
				Aliases: []string{"d"},
				Usage: "データを取り出すデータベースの名前 `DBNAME` を指定します。",
				Destination: &dbname,
				Required: true,
			},
			&cli.StringFlag{
				Name: "sqlF",
				Aliases: []string{"v"},
				Usage: "SQLを記述したファイルのパス名(絶対パス) `SQL_FILE_PATH` を指定します。",
				Destination: &sqlF,
			},
			&cli.StringFlag{
				Name: "sql",
				Aliases: []string{"s"},
				Usage: "`SQL` を指定します。",
				Destination: &sql,
			},
			&cli.StringFlag{
				Name: "input",
				Aliases: []string{"i"},
				Usage: "データを入力するファイルのパス `INPUT_FILE_PATH` を指定します。",
				Destination: &input,
				Required: true,
			},
			&cli.StringFlag{
				Name: "output",
				Aliases: []string{"o"},
				Usage: "データを出力するファイルのパス `OUTPUT_FILE_PATH` を指定します。",
				Destination: &output,
				Required: true,
			},
		},
		Before: func(c *cli.Context) error {
			if !c.IsSet("sqlF") && !c.IsSet("sql") {
				ec := cli.Exit("sqlF か sql のどちらかを指定する必要があります。", 1)
				return ec
			}
		}
		Action: func(c *cli.Context) error {
			name := "Nefertiti"
			if c.NArg() > 0 {
				name = c.Args().Get(0)
			}
			if c.String("lang") == "spanish" {
				fmt.Println("Hola", name)
			} else {
				fmt.Println("Hello", name)
			}
			return nil
			// ec := cli.Exit("ohwell", 86)
			// fmt.Fprintf(c.App.Writer, "%d", ec.ExitCode())
			// fmt.Printf("made it!\n")
			// return ec

			// チャネル src から受信した rdbunlsql の実行に必要な情報を基に、rdbunlsql を実行する。
			// rdbunlsql の実行は Workers パターンを使用するが、チャネル src の順番で w に出力する
			// ために、バッファありチャネル dataChs を使用する。

			// rdbunlsql の実行結果を直列化して書き出すためのチャネル
			dataChs := make(chan chan []byte, 50)
			// 実行対象の rdbunlsql 情報をためるチャネル
			unlsqls := make(chan Unlsql, 50)

			// rdbunlsql実行結果を直列化して書き出す専用のゴルーチン
			done := make(chan struct{})
			go writeInOrder(dataChs, w, done)

			// CPU数に応じたワーカーを生成
			// ワーカーの数をCPU数の半分にしているのは、CPU数の半分の方が処理速度が早いと書いてある
			// サイトがあったから
			var wg sync.WaitGroup
			for i := 0; i < runtime.NumCPU()/2; i++ {
				wg.Add(1)
				go worker(i+1, unlsqls, errCh, &wg)
			}

			// src から受信したrdbunlsql情報を、ワーカーに渡すためのチャネル unlsqls と dataChs に
			// 格納していく。
			go func() {
				for u := range src {
					dataChs <- u.dataCh
					unlsqls <- u
				}
				close(dataChs)
				close(unlsqls)
			}()

			wg.Wait()

			<-done

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func rdbunlsql(db string, sql string, path string) error {
	out, err  := exec.Command("rdbunlsql", "-d", db, "-s", sql, "-t", path).CombinedOutput();
	if err != nil {
		return fmt.Errorf("failed to rdbunlsql: %w, msg: %s", err, out)
	}

	return nil
}

func createSearchMap(r io.Reader) (map[string]string, error) {
	m := make(map[string]string)

 	s := bufio.NewScanner(r)

 	for s.Scan() {
 		ary := strings.Split(s.Text(), ",")

		if len(ary) != 7 {
			return nil, fmt.Errorf("テンポラリストレージのファイルリストのフォーマット不正. len=%d", len(ary))
		}

		// パスの区切りは「/」とする
		p := strings.Replace(ary[1], "\\", "/", -1)
		m[p] = ary[3]
 	}

	if s.Err() != nil {
		// non-EOF error.
		return nil, s.Err()
	}

	return m, nil
}

func gen(r io.Reader, letter string) (<-chan File) {
	out := make(chan File, 50)

	go func(br *bufio.Reader) {
		defer close(out)
		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			ary := strings.Split(s.Text(), ",")
			aryP := ary[0:3]
			size := ary[4]
			path := strings.Join(aryP, "/")

			select {
			case out <- File{path, size}:
			default:
				fmt.Println("入力ファイル用チャネルのバッファが不足しています.")
			}
		}

		if scanner.Err() != nil {
			// non-EOF error.
			return nil, scanner.Err()
		}
	}(bufio.NewReader(br))

	return out
}

// ワーカー
func worker(i int, db string, src <-chan Unlsql, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	// タスクがなくなってタスクのチェネルがcloseされるまで無限ループ
	for s := range src {
		out, err := filepath.Abs(fmt.Sprintf("./data_%d", i)
		if err != nil {
			errCh <- err
			return
		}
		if err := rdbunlsql(db, s.sql, out) {
			errCh <- err
			return
		}
		bytes, err := ioutil.ReadFile(out)
		if err != nil {
			errCh <- err
			return
		}
		s.dataCh <- bytes
	}
}

// 順番に従ってに書き出しをする(goroutineで実行される)
func writeInOrder(dataChs <-chan chan []byte, w io.Writer, done chan<- struct{}) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	// 書き出し完了を表すチャネルをクローズする
	defer close(done)

	// 順番に取り出す
	for dataCh := range dataChs {
		// 選択された仕事が終わるまで待つ
		data := <-dataCh
		if _, err := bw.Write(data); err != nil {
			return err
		}
		close(dataCh)
	}

	return nil
}

// func main() {
// 	fmt.Printf("runtime.NumCPU()=%d\n", runtime.NumCPU())

// 	m := bytes.NewBufferString(conmap)
// 	conmap, err := genConvertMap(m)

// 	//filename := os.Args[1]
// 	//f, err := os.Open(filename)
// 	//if err != nil {
// 	//	log.Fatalf("cannot open file %q: %v", filename, err)
// 	//}
// 	//defer f.Close()
// 	f := bytes.NewBufferString(target)
// 	jefs, err := gen(f)
// 	if err != nil {
// 		log.Fatalf("cannot generate convert map: %v", err)
// 	}

// 	stdout := new(bytes.Buffer)
// 	processSession(jefs, conmap, stdout)
// 	fmt.Printf("%s -> %s", target, stdout.String())
// }