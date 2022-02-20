package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/urfave/cli/v2"
)

type File struct {
	path string // ファイルパス
	size string // ファイルサイズ
}

type Unmatch struct {
	path   string // ファイルのパス
	reason string // 不一致理由
}

const (
	UnmatchReasonNonExist    = "ファイルなし"
	UnmatchReasonSizeUnmatch = "ファイルサイズ不一致"
)

func main() {
	var baseDir, source, dest, output, ignore string
	var numConcret, verbose int

	app := &cli.App{
		Name:    "pjkakuninja",
		Version: Version,
		Usage:   "P-WEB確認ジャ！",
		Commands: []*cli.Command{
			{
				Name:    "check",
				Aliases: []string{"a"},
				Usage:   "ファイルマッチング",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:        "num-concrent",
						Aliases:     []string{"c"},
						Usage:       "並列処理数 `NUM_CONCRENT` を指定します。未指定の場合、CPU数の半分が設定されます。",
						Destination: &numConcret,
					},
					&cli.StringFlag{
						Name:        "baseDir",
						Aliases:     []string{"b"},
						Usage:       "チェック先フォルダのパス `BASE_DIR` を指定します。",
						Destination: &baseDir,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "source",
						Aliases:     []string{"s"},
						Usage:       "比較元ファルのパス `SOURCE_FILE_PATH` を指定します。",
						Destination: &source,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "dest",
						Aliases:     []string{"d"},
						Usage:       "比較先ファルのパス `DEST_FILE_PATH` を指定します。",
						Destination: &dest,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "output",
						Aliases:     []string{"o"},
						Usage:       "データを出力するファイルのパス `OUTPUT_FILE_PATH` を指定します。",
						Destination: &output,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "ignore",
						Aliases:     []string{"g"},
						Usage:       "比較元ファイルのファイル名が `IGNORE` を含む場合、除外します。",
						Destination: &ignore,
					},
				},
				Action: func(c *cli.Context) error {
					// チェック結果を出力するファイル。既にファイルが存在する場合は削除
					outFp, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer outFp.Close()

					// チェック結果を書き出す専用のゴルーチン
					resultsCh := make(chan Unmatch, 50) // アンマッチファイルを書き出すためのチャネル
					done := make(chan struct{})         // ファイル出力終了を伝えるためのチャネル
					go writeUnMatchFile(resultsCh, outFp, done)

					// チェック先のファイル
					destFp, err := os.Open(dest)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer destFp.Close()

					// チェック先ファイルからチェック用のハッシュマップを生成する
					destMap, err := createDestMap(destFp)
					if err != nil {
						return cli.Exit(err, 1)
					}

					// チェック元
					srcFp, err := os.Open(source)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer srcFp.Close()
					sourceCh := gen(srcFp, baseDir, ignore)

					// NUM_CONCURRENT が未指定の場合は、CPU数の半分とする。
					if numConcret == 0 {
						numConcret = runtime.NumCPU() / 2
					}

					// ワーカーを生成
					var wg sync.WaitGroup
					for i := 0; i < numConcret; i++ {
						wg.Add(1)
						go worker(sourceCh, destMap, resultsCh, &wg)
					}
					wg.Wait()

					// ワーカーがすべて完了すると、resultsCh への送信が完了するのでクローズする
					close(resultsCh)

					// writeUnMatchFile が完了するまで待機
					<-done

					return nil
				},
			},
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "ファイルリスト作成",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "baseDir",
						Aliases:     []string{"b"},
						Usage:       "チェック先フォルダのパス `BASE_DIR` を指定します。",
						Destination: &baseDir,
						Required:    true,
					},
					&cli.StringFlag{
						Name:        "output",
						Aliases:     []string{"o"},
						Usage:       "データを出力するファイルのパス `OUTPUT_FILE_PATH` を指定します。",
						Destination: &output,
						Required:    true,
					},
					&cli.IntFlag{
						Name:        "verbose",
						Aliases:     []string{"V"},
						Usage:       "中間件数の出力件数を指定します。",
						Destination: &verbose,
					},
				},
				Action: func(c *cli.Context) error {
					outFp, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer outFp.Close()

					bw := bufio.NewWriter(outFp)
					defer bw.Flush()

					count := 0
					err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
						if err != nil {
							return err
						}

						if path == baseDir {
							return nil
						}

						filename := info.Name() // ファイル名
						ext := ""               // ファイルの拡張子
						size := info.Size()     // ファイルサイズ
						folderFlag := "FALSE"
						if info.IsDir() {
							folderFlag = "TRUE"
						}
						updateDate := info.ModTime().Format("2006/01/02") // 更新日
						updateTime := info.ModTime().Format("15:04:05")   // 更新時刻

						// "ファイル名","ファイルのフルパス","ファイルの拡張子",ファイルサイズ,フォルダフラグ(フォルダの場合TRUE),更新日,更新時刻
						s := fmt.Sprintf("\"%s\",\"%s\",\"%s\",%d,%s,%s,%s\n", filename, path, ext, size, folderFlag, updateDate, updateTime)
						if _, err := bw.WriteString(s); err != nil {
							return err
						}

						count += 1
						if verbose != 0 && count%verbose == 0 {
							fmt.Printf("%d 完了...\n", count)
						}

						return nil
					})

					if err != nil {
						return err
					}

					fmt.Printf("%s のファイルリストの作成完了(件数=%d)\n", baseDir, count)
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// r で指定されたファイルから、チェック用のマップを生成する。
// r の1行は次の構成。
// "ファイル名","ファイルのフルパス","ファイルの拡張子",ファイルサイズ,フォルダフラグ(フォルダの場合TRUE),更新日,更新時刻
func createDestMap(r io.Reader) (map[string]string, error) {
	m := make(map[string]string)
	var read, skip, add uint

	s := bufio.NewScanner(r)
	for s.Scan() {
		read += 1
		if s.Text() == "" {
			skip += 1
			continue
		}

		ary := strings.Split(s.Text(), ",")
		if len(ary) != 7 {
			return nil, fmt.Errorf("テンポラリストレージのファイルリストのフォーマット不正. len=%d", len(ary))
		}

		// フォルダフラグが "TRUE" の場合はチェック対象外のためスキップする
		if ary[4] == "TRUE" {
			skip += 1
			continue
		}

		// ファイルパスが”で括られているので削除する。 パスの区切りは「/」とする
		p := filepath.ToSlash(strings.Replace(ary[1], "\"", "", -1))
		m[p] = ary[3]

		add += 1
	}

	if s.Err() != nil {
		// non-EOF error.
		return nil, s.Err()
	}

	// ファイル読み込み結果を出力
	fmt.Println("◆チェック先ファイル(DEST_FILE_PATH)の読み込みを完了しました。")
	fmt.Printf("　→読み込み件数 : %d\n", read)
	fmt.Printf("　→検索用ファイル件数 : %d\n", add)
	fmt.Printf("　→スキップ件数(ディレクトリ) : %d\n", skip)

	return m, nil
}

// rで指定されたファイルを1行ずつ読み込み、File のチャネルを生成する。
// rの1行の構成は次の通り。
// "プロジェクト名","カテゴリ","サブカテゴリ",ファイルパス,ファイルサイズ
func gen(r io.Reader, prifix, ignore string) <-chan File {
	out := make(chan File, 50) // バッファ数50の根拠はなし

	b := newBufioReader(r)
	p := modifySourcePathPrifix(prifix)

	go func(br *bufio.Reader, p string) {
		defer close(out)

		var read, skip, add uint

		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			read += 1

			ary := strings.Split(scanner.Text(), ",")

			// 無視するファイルのチェック
			if ignore != "" && strings.Contains(ary[3], ignore) {
				skip += 1
				continue
			}

			// ファイルパスの作成。”で括られているので削除する。
			aryP := ary[0:4]
			size := ary[4]
			path := p + strings.Replace(strings.Join(aryP, "/"), "\"", "", -1)

			out <- File{path, size}
			add += 1
		}

		// 横着してエラーメッセージの出力のみで終わっている。。。
		// 本当は、ワーカーやファイル出力のゴルーチンを終了させる必要がある。
		if scanner.Err() != nil {
			// non-EOF error.
			fmt.Printf("チェック元ファイルの読み込みでエラーが発生しました.(%s)\n", scanner.Err())
			return
		}

		// ファイル読み込み結果を出力する。
		fmt.Println("◆チェック元ファイル(SOURCE_FILE_PATH)の読み込みを完了しました。")
		fmt.Printf("　→読み込み件数 : %d\n", read)
		fmt.Printf("　→スキップ件数 : %d\n", skip)
		fmt.Printf("　→検索対象ファイル件数 : %d\n", add)
	}(b, p)

	return out
}

// s の "\" を "/" に置換する。置換した結果、末尾に "/" がない場合は付加する。
func modifySourcePathPrifix(s string) string {
	prifix := strings.Replace(s, "\\", "/", -1)
	if !strings.HasSuffix(prifix, "/") {
		prifix = prifix + "/"
	}
	return prifix
}

// ワーカー
func worker(fileCh <-chan File, destMap map[string]string, resultsCh chan<- Unmatch, wg *sync.WaitGroup) {
	defer wg.Done()
	// タスクがなくなってタスクのチェネルがcloseされるまで無限ループ
	for f := range fileCh {
		isExist := true
		msg := ""
		if v, ok := destMap[f.path]; ok {
			if v != f.size {
				isExist = false
				msg = UnmatchReasonSizeUnmatch
			}
		} else {
			isExist = false
			msg = UnmatchReasonNonExist
		}

		if !isExist {
			resultsCh <- Unmatch{f.path, msg}
			// fmt.Printf("%s:%s\n", msg, f.path)
		}
	}
}

// w へアンマッチファイルのパスを出力する(goroutineで実行される)
func writeUnMatchFile(resultsCh <-chan Unmatch, w io.Writer, done chan<- struct{}) error {
	// 書き出し完了を表すチャネルをクローズする
	defer close(done)

	var write, nonexists, sizeunmatch uint

	bw := bufio.NewWriter(w)
	defer bw.Flush()

	// resultsCh が close するまで繰り返す
	for p := range resultsCh {
		if _, err := bw.WriteString(fmt.Sprintf("%s,%s\n", p.reason, p.path)); err != nil {
			return err
		}
		write += 1
		switch p.reason {
		case UnmatchReasonNonExist:
			nonexists += 1
		case UnmatchReasonSizeUnmatch:
			sizeunmatch += 1
		}
	}

	// 結果を出力
	fmt.Println("◆結果ファイル(OUTPUT_FILE_PATH)の書き込みを完了しました。")
	fmt.Printf("　→出力件数 : %d\n", write)
	fmt.Printf("　→ファイルなし : %d\n", nonexists)
	fmt.Printf("　→サイズ不一致 : %d\n", sizeunmatch)

	return nil
}

func newBufioReader(r io.Reader) *bufio.Reader {
	br := bufio.NewReader(r)
	bs, err := br.Peek(3)
	if err != nil {
		return br
	}
	if bs[0] == 0xEF && bs[1] == 0xBB && bs[2] == 0xBF {
		br.Discard(3)
	}
	return br
}
