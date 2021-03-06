package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/saracen/walker"
	"github.com/urfave/cli/v2"
)

type File struct {
	path         string    // ファイルパス
	size         int       // ファイルサイズ
	dateModified time.Time // 更新日時
}

type SizeAndDateModified struct {
	Size            int
	DateModified    time.Time
	SizeOld         int
	DateModifiedOld time.Time
}

type Unmatch struct {
	path   string // ファイルのパス
	reason string // 不一致理由
}

const (
	UnmatchReasonNonExist          = "ファイルなし"
	UnmatchReasonSizeUnmatch       = "ファイルサイズ不一致"
	UnmatchReasonSizeShrink        = "ファイルサイズ縮小"
	UnmatchReasonDateModifiedError = "ファイル更新日時エラー"
)

const (
	compareModeSizeEq         = iota // サイズ一致
	compareModeSizeGe                // サイズ以上なら真
	compareModeSizeGeAndModGe        // サイズ以上 & 更新日時未来
)

func main() {
	var baseDir, spoDir, source, dest, destOld, output, ignore, recovery, spopath, trimWord string
	var numConcret, verbose int

	app := &cli.App{
		Name:    "pjkakuninja",
		Version: Version,
		Usage:   "P-WEB確認ジャ！",
		Commands: []*cli.Command{
			{
				Name:    "check-temp",
				Aliases: []string{"ct"},
				Usage:   "TEMPストレージのファイルマッチング",
				Flags: []cli.Flag{
					opsNumConcent(&numConcret),
					opsBaseDir(&baseDir),
					opsSource(&source),
					opsDest(&dest),
					opsDestOld(&destOld),
					opsOutput(&output),
					opsIgnore(&ignore),
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

					// チェック先ファイルからチェック用のハッシュマップを生成する
					destMap, err := generateDestMapFromTempFileListPath(dest, destOld)
					if err != nil {
						return cli.Exit(err, 1)
					}

					// チェック元
					srcFp, err := os.Open(source)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer srcFp.Close()
					sourceCh := generateSourceFromPJFileList(srcFp, baseDir, ignore)

					// NUM_CONCURRENT が未指定の場合は、CPU数の半分とする。
					newNumConcrent := getNumConcrent(numConcret)

					// ワーカーを生成
					var wg sync.WaitGroup
					for i := 0; i < newNumConcrent; i++ {
						wg.Add(1)
						go worker(sourceCh, destMap, resultsCh, compareModeSizeEq, &wg)
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
					filelistCh := make(chan string, 50)
					done := make(chan struct{})
					go writeFilePathList(filelistCh, output, done, verbose)

					err := walker.Walk(baseDir, func(path string, info os.FileInfo) error {
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
						s := fmt.Sprintf("\"%s\",\"%s\",\"%s\",%d,%s,%s,%s", filename, path, ext, size, folderFlag, updateDate, updateTime)
						filelistCh <- s

						return nil
					})
					close(filelistCh)

					if err != nil {
						return err
					}

					<-done

					return nil
				},
			},
			{
				Name:    "check-spo",
				Aliases: []string{"cs"},
				Usage:   "SPOのファイルマッチング",
				Flags: []cli.Flag{
					opsNumConcent(&numConcret),
					opsBaseDir(&baseDir),
					opsSPODir(&spoDir),
					opsSource(&source),
					opsDest(&dest),
					opsOutput(&output),
					opsIgnore(&ignore),
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

					// チェック先ファイルからチェック用のハッシュマップを生成する
					destMap, err := generateDestMapFromSPOFileListPath(dest, baseDir, spoDir)
					if err != nil {
						return cli.Exit(err, 1)
					}

					// チェック元
					srcFp, err := os.Open(source)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer srcFp.Close()
					sourceCh := generateSourceFromTempFileList(srcFp, ignore)

					// NUM_CONCURRENT が未指定の場合は、CPU数の半分とする。
					newNumConcrent := getNumConcrent(numConcret)

					// ワーカーを生成
					var wg sync.WaitGroup
					for i := 0; i < newNumConcrent; i++ {
						wg.Add(1)
						go worker(sourceCh, destMap, resultsCh, compareModeSizeGeAndModGe, &wg)
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
				Name:    "recovery-spo",
				Aliases: []string{"r"},
				Usage:   "SPOへの再送信",
				Flags: []cli.Flag{
					opsRecovery(&recovery),
					opsOutput(&output),
					opsTrimWord(&trimWord),
					opsSpopath(&spopath),
				},
				Action: func(c *cli.Context) error {
					// リカバリリスト
					recFp, err := os.Open(recovery)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer recFp.Close()

					// チェック結果を出力するファイル。既にファイルが存在する場合は削除
					outFp, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer outFp.Close()

					bw := bufio.NewWriter(outFp)
					defer bw.Flush()

					var read, write uint

					s := bufio.NewScanner(recFp)
					for s.Scan() {
						read += 1

						ary := strings.Split(s.Text(), ",")
						if len(ary) != 2 {
							return fmt.Errorf("ファイルリストのフォーマット不正. len=%d", len(ary))
						}

						filePath := ary[1]
						dirPath := filePath[strings.Index(filePath, "/"):strings.LastIndex(filePath, "/")]
						if trimWord != "" {
							if !strings.HasPrefix(trimWord, "/") {
								trimWord = "/" + trimWord
							}
							dirPath = strings.TrimPrefix(dirPath, trimWord)
						}
						if spopath != "" {
							if !strings.HasPrefix(spopath, "/") {
								spopath = "/" + spopath
							}
							dirPath = fmt.Sprintf("%s%s", spopath, dirPath)
						}

						AddPngCmd := fmt.Sprintf("Add-PnPFile -Path \"%s\" -Folder \"Shared%%20Documents%s\"\n", strings.Replace(filePath, "$", "`$", -1), strings.Replace(dirPath, "$", "`$", -1))

						if _, err := bw.WriteString(AddPngCmd); err != nil {
							return err
						}
						write += 1
					}

					if s.Err() != nil {
						// non-EOF error.
						return s.Err()
					}

					fmt.Println("◆リカバリファイル(RECOVERY_FILE_PATH)の出力を完了しました。")
					fmt.Printf("　→ファイル入力件数 : %d\n", read)
					fmt.Printf("　→ファイル出力件数 : %d\n", write)

					return nil
				},
			},
			{
				Name:    "dummy-temp-list",
				Aliases: []string{"d"},
				Usage:   "ダミーのテンポラリストレージファイルリスト作成",
				Flags: []cli.Flag{
					opsSource(&source),
					opsOutput(&output),
					opsDestNonRequired(&dest),
					opsDestOld(&destOld),
					opsBaseDir(&baseDir),
				},
				Action: func(c *cli.Context) error {
					// PJWEBリスト
					srcFp, err := os.Open(source)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer srcFp.Close()

					// チェック結果を出力するファイル。既にファイルが存在する場合は削除
					outFp, err := os.OpenFile(output, os.O_CREATE|os.O_TRUNC, 0644)
					if err != nil {
						return cli.Exit(err, 1)
					}
					defer outFp.Close()

					bw := bufio.NewWriter(outFp)
					defer bw.Flush()

					// チェック先ファイルからチェック用のハッシュマップを生成する
					var destMap map[string]*SizeAndDateModified
					if dest != "" {
						destMap, err = generateDestMapFromTempFileListPath(dest, destOld)
						if err != nil {
							return cli.Exit(err, 1)
						}
					}

					var read, write uint

					// "プロジェクト名","カテゴリ","サブカテゴリ",ファイルパス,ファイルサイズ
					s := bufio.NewScanner(srcFp)
					for s.Scan() {
						read += 1

						ary := strings.Split(s.Text(), ",")
						if len(ary) != 5 {
							return fmt.Errorf("ファイルリストのフォーマット不正. len=%d", len(ary))
						}

						// ファイルパスの作成。”で括られているので削除する。
						aryP := ary[0:4]
						path := modifySourcePathPrifix(baseDir) + strings.Replace(strings.Join(aryP, "/"), "\"", "", -1)
						filename := filepath.Base(path)
						extname := strings.TrimLeft(filepath.Ext(path), ".")
						size := ary[4]
						updateDate := "2022/3/5"
						updateTime := "15:04:05"
						if v, ok := destMap[strings.ToLower(path)]; ok {
							if v.SizeOld != 0 {
								updateDate = v.DateModifiedOld.Format("2006/01/02")
								if v.DateModifiedOld.Hour() < 12 {
									updateTime = v.DateModifiedOld.Format("3:04:05")
								} else {
									updateTime = v.DateModifiedOld.Format("15:04:05")
								}
							} else {
								updateDate = v.DateModified.Format("2006/01/02")
								if v.DateModified.Hour() < 12 {
									updateTime = v.DateModified.Format("3:04:05")
								} else {
									updateTime = v.DateModified.Format("15:04:05")
								}
							}
						}

						// "ファイル名","ファイルのフルパス","ファイルの拡張子",ファイルサイズ,フォルダフラグ(フォルダの場合TRUE),更新日(YYYY/MM/DD),更新時刻(hh:mm:dd)
						out := fmt.Sprintf("\"%s\",\"%s\",\"%s\",%s,FALSE,%s,%s\r\n", filename, strings.Replace(path, "/", "\\", -1), extname, size, updateDate, updateTime)
						if _, err := bw.WriteString(out); err != nil {
							return err
						}
						write += 1
					}

					if s.Err() != nil {
						// non-EOF error.
						return s.Err()
					}

					fmt.Println("◆リカバリファイル(RECOVERY_FILE_PATH)の出力を完了しました。")
					fmt.Printf("　→ファイル入力件数 : %d\n", read)
					fmt.Printf("　→ファイル出力件数 : %d\n", write)

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
func generateDestMapFromTempFileList(rBe io.Reader) (map[string]*SizeAndDateModified, error) {
	m := make(map[string]*SizeAndDateModified)
	var readBe, skipBe, addBe uint

	// 処理前ファイル
	sBe := bufio.NewScanner(rBe)
	for sBe.Scan() {
		readBe += 1
		if sBe.Text() == "" {
			skipBe += 1
			continue
		}

		ary := strings.Split(sBe.Text(), ",")
		if len(ary) != 7 {
			return nil, fmt.Errorf("ファイルリストのフォーマット不正. len=%d", len(ary))
		}

		// フォルダフラグが "TRUE" の場合はチェック対象外のためスキップする
		if ary[4] == "TRUE" {
			skipBe += 1
			continue
		}

		// ファイルパスが”で括られているので削除する。 パスの区切りは「/」とする
		p := filepath.ToSlash(strings.Replace(ary[1], "\"", "", -1))
		size, _ := strconv.Atoi(ary[3])
		d, err := time.Parse("2006/01/02 15:04:05", ary[5]+" "+ary[6])
		if err != nil {
			d = time.Time{}
		}
		m[strings.ToLower(p)] = &SizeAndDateModified{size, d, 0, time.Time{}}
		// m[strings.ToLower(p)] = &SizeAndDateModified{size, time.Time{}, 0, time.Time{}}

		addBe += 1
	}

	if sBe.Err() != nil {
		// non-EOF error.
		return nil, sBe.Err()
	}

	fmt.Println("◆チェック先ファイル(DEST_FILE_PATH)の読み込みを完了しました。")
	fmt.Printf("　→ファイル読み込み件数 : %d\n", readBe)
	fmt.Printf("　→検索用ファイル件数 : %d\n", addBe)
	fmt.Printf("　→スキップ件数(ディレクトリ) : %d\n", skipBe)

	return m, nil
}

func updateDestMapFromTempFileList(m map[string]*SizeAndDateModified, rAf io.Reader) (map[string]*SizeAndDateModified, error) {
	var readAf, skipAf, updateAf uint

	// 処理後ファイル
	sAf := bufio.NewScanner(rAf)
	for sAf.Scan() {
		readAf += 1
		if sAf.Text() == "" {
			skipAf += 1
			continue
		}

		ary := strings.Split(sAf.Text(), ",")
		if len(ary) != 7 {
			return nil, fmt.Errorf("ファイルリストのフォーマット不正. len=%d", len(ary))
		}

		// フォルダフラグが "TRUE" の場合はチェック対象外のためスキップする
		if ary[4] == "TRUE" {
			skipAf += 1
			continue
		}

		// ファイルパスが”で括られているので削除する。 パスの区切りは「/」とする
		p := filepath.ToSlash(strings.Replace(ary[1], "\"", "", -1))
		s, _ := strconv.Atoi(ary[3])
		d, err := time.Parse("2006/01/02 15:04:05", ary[5]+" "+ary[6])
		if err != nil {
			d = time.Time{}
		}

		if v, ok := m[strings.ToLower(p)]; ok {
			v.SizeOld = s
			v.DateModifiedOld = d
			updateAf += 1
		}
	}

	if sAf.Err() != nil {
		// non-EOF error.
		return nil, sAf.Err()
	}

	// ファイル読み込み結果を出力
	fmt.Println("◆チェック先ファイル(秘密度前)(DEST_FILE_OLD_PATH)の読み込みを完了しました。")
	fmt.Printf("　→ファイル読み込み件数 : %d\n", readAf)
	fmt.Printf("　→更新件数 : %d\n", updateAf)
	fmt.Printf("　→スキップ件数(ディレクトリ) : %d\n", skipAf)

	return m, nil
}

// r で指定されたファイルから、チェック用のマップを生成する。
// r の1行は次の構成。
// 0:          1:               2:      3:              4:                                           5:
// "ファイル名","更新日 更新時刻(YYYY/MM/MM h:mm:dd)","更新者","ファイルサイズ","ファイル区分(フォルダ=Folder、ファイル=File)","格納フォルダのパス"
func generateDestMapFromSPOFileList(r io.Reader, prifix, sd string) (map[string]*SizeAndDateModified, error) {
	m := make(map[string]*SizeAndDateModified)
	var read, skip, add uint

	p := modifySourcePathPrifix(prifix)

	b := newBufioReader(r)
	s := bufio.NewScanner(b)

	for s.Scan() {
		read += 1

		// 1行目は項目名なのでスキップ
		if read == 1 {
			skip += 1
			continue
		}

		ary := strings.Split(s.Text(), ",")
		if len(ary) != 6 {
			return nil, fmt.Errorf("ファイルリストのフォーマット不正. len=%d, line=%s", len(ary), s.Text())
		}

		// ファイル区分が "Folder" の場合はチェック対象外のためスキップする
		if ary[4] == "Folder" {
			skip += 1
			continue
		}

		// ファイルパスの生成
		pathAndFile := strings.Replace(ary[5]+"/"+ary[0], "\"", "", -1) // "を削除
		path := p + strings.Replace(pathAndFile, sd, "", -1)

		// ファイルサイズ
		size, _ := strconv.Atoi(strings.Replace(ary[3], "\"", "", -1)) // "を削除

		// 更新日時("YYYY/MM/MM h:mm:dd")
		d, err := time.Parse("2006/01/02 15:04:05", strings.Replace(ary[1], "\"", "", -1))
		if err != nil {
			return nil, err
		}
		d = d.Add(9 * time.Hour) // 9時間加算

		// SPOへアップロードすると大文字に（勝手に）変換される場合があるので、キーは小文字に変換する
		m[strings.ToLower(path)] = &SizeAndDateModified{size, d, 0, time.Time{}}

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
	fmt.Printf("　→スキップ件数: %d\n", skip)

	return m, nil
}

// rで指定されたファイルを1行ずつ読み込み、File のチャネルを生成する。
// rの1行の構成は次の通り。
// "プロジェクト名","カテゴリ","サブカテゴリ",ファイルパス,ファイルサイズ
func generateSourceFromPJFileList(r io.Reader, prifix, ignore string) <-chan File {
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
			size, _ := strconv.Atoi(ary[4])
			path := p + strings.Replace(strings.Join(aryP, "/"), "\"", "", -1)

			out <- File{path, size, time.Time{}}
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

// rで指定されたファイルを1行ずつ読み込み、File のチャネルを生成する。
// rの1行の構成は次の通り。
// 0:          1:                  2:               3:            4:                              5:                 6:
// "ファイル名","ファイルのフルパス","ファイルの拡張子",ファイルサイズ,フォルダフラグ(フォルダの場合TRUE),更新日(YYYY/MM/DD),更新時刻(hh:mm:dd)
func generateSourceFromTempFileList(r io.Reader, ignore string) <-chan File {
	out := make(chan File, 50) // バッファ数50の根拠はなし

	b := newBufioReader(r)

	go func(br *bufio.Reader) {
		defer close(out)

		var read, dirSkip, invalidSlip, ingSkip, add uint

		scanner := bufio.NewScanner(br)
		for scanner.Scan() {
			read += 1

			ary := strings.Split(scanner.Text(), ",")

			// フォルダフラグが "TRUE" の場合はチェック対象外のためスキップする
			if ary[4] == "TRUE" {
				dirSkip += 1
				continue
			}

			// ファイルサイズ
			size, _ := strconv.Atoi(ary[3])

			// ファイル名が「~$」で始まるファイル、かつ、200バイト未満は対象外のためスキップする
			// Thumbs.db もスキップする
			if (strings.HasPrefix(ary[0], `"~$`) && size < 200) || ary[0] == "\"Thumbs.db\"" {
				invalidSlip += 1
				continue
			}

			// 無視するファイルのチェック
			if ignore != "" && strings.Contains(ary[1], ignore) {
				ingSkip += 1
				continue
			}

			// ファイルパスが”で括られているので削除する。 パスの区切りは「/」とする
			p := filepath.ToSlash(strings.Replace(ary[1], "\"", "", -1))
			d, err := time.Parse("2006/01/02 15:04:05", ary[5]+" "+ary[6])
			if err != nil {
				d = time.Time{}
			}

			out <- File{p, size, d}
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
		fmt.Printf("　→スキップ件数(フォルダ) : %d\n", dirSkip)
		fmt.Printf("　→スキップ件数(無視ファイル) : %d\n", ingSkip)
		fmt.Printf("　→スキップ件数(無効ファイル)： %d\n", invalidSlip)
		fmt.Printf("　→検索対象ファイル件数 : %d\n", add)
	}(b)

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
func worker(fileCh <-chan File, destMap map[string]*SizeAndDateModified, resultsCh chan<- Unmatch, compareMode int, wg *sync.WaitGroup) {
	defer wg.Done()

	// タスクがなくなってタスクのチェネルがcloseされるまで無限ループ
	for f := range fileCh {
		isExist := true
		msg := ""
		if v, ok := destMap[strings.ToLower(f.path)]; ok {
			switch compareMode {
			case compareModeSizeEq:
				if v.Size != f.size && v.SizeOld != f.size {
					isExist = false
					msg = UnmatchReasonSizeUnmatch
				}
			case compareModeSizeGe:
				if v.Size < f.size {
					isExist = false
					msg = UnmatchReasonSizeShrink
				}
			case compareModeSizeGeAndModGe:
				// 比較先のファイルサイズは、比較元のファイルサイズ以上であるのが正しい
				if v.Size < f.size {
					// fmt.Printf("比較元サイズ:%d, 比較先サイズ:%d\n", f.size, v.beforeSize)
					isExist = false
					msg = UnmatchReasonSizeShrink
				} else {
					// 比較先の更新日時は、比較元の更新日時より未来であるのが正しい
					if v.DateModified.Unix() <= f.dateModified.Unix() {
						// fmt.Printf("比較元更新日時:%s, 比較先更新日時:%s\n", f.dateModified.Format("2006/01/02 15:04:05"), v.beforeDateModified.Format("2006/01/02 15:04:05"))
						isExist = false
						msg = UnmatchReasonDateModifiedError
					}
				}
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

	var write, nonexists, sizeunmatch, sizeshrink, dateModified uint

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
		case UnmatchReasonSizeShrink:
			sizeshrink += 1
		case UnmatchReasonDateModifiedError:
			dateModified += 1
		}
	}

	// 結果を出力
	fmt.Println("◆結果ファイル(OUTPUT_FILE_PATH)の書き込みを完了しました。")
	fmt.Printf("　→出力件数 : %d\n", write)
	fmt.Printf("　→ファイルなし : %d\n", nonexists)
	fmt.Printf("　→サイズ不一致 : %d\n", sizeunmatch)
	fmt.Printf("　→サイズ縮小 : %d\n", sizeshrink)
	fmt.Printf("　→更新日時エラー : %d\n", dateModified)

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

func writeFilePathList(filepathCh <-chan string, outfile string, done chan<- struct{}, verbose int) error {
	// 書き出し完了を表すチャネルをクローズする
	defer close(done)

	count := 0

	outFp, err := os.OpenFile(outfile, os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer outFp.Close()

	bw := bufio.NewWriter(outFp)
	defer bw.Flush()

	// resultsCh が close するまで繰り返す
	for p := range filepathCh {
		if _, err := bw.WriteString(fmt.Sprintf("%s\n", p)); err != nil {
			return err
		}
		count += 1
		if verbose != 0 && count%verbose == 0 {
			fmt.Printf("%d 件完了...\n", count)
		}

	}

	// 結果を出力
	fmt.Println("◆ファイルリストの出力を完了しました。")
	fmt.Printf("　→出力件数 : %d\n", count)

	return nil
}

func opsNumConcent(n *int) *cli.IntFlag {
	return &cli.IntFlag{
		Name:        "num-concrent",
		Aliases:     []string{"c"},
		Usage:       "並列処理数 `NUM_CONCRENT` を指定します。未指定の場合、CPU数の半分が設定されます。",
		Destination: n,
	}
}

func opsBaseDir(b *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "baseDir",
		Aliases:     []string{"b"},
		Usage:       "チェック先フォルダのパス `BASE_DIR` を指定します。",
		Destination: b,
		Required:    true,
	}

}

func opsSPODir(b *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "spoDir",
		Aliases:     []string{"q"},
		Usage:       "SPOのフォルダパス `SPO_DIR` を指定します。",
		Destination: b,
		Required:    true,
	}
}

func opsSource(s *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "source",
		Aliases:     []string{"s"},
		Usage:       "比較元ファルのパス `SOURCE_FILE_PATH` を指定します。",
		Destination: s,
		Required:    true,
	}
}

func opsDest(d *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "dest",
		Aliases:     []string{"d"},
		Usage:       "比較先ファルのパス `DEST_FILE_PATH` を指定します。",
		Destination: d,
		Required:    true,
	}
}

func opsDestNonRequired(d *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "dest",
		Aliases:     []string{"d"},
		Usage:       "比較先ファルのパス `DEST_FILE_PATH` を指定します。",
		Destination: d,
	}
}
func opsDestOld(d *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "destAf",
		Aliases:     []string{"a"},
		Usage:       "比較先ファル(処理後)のパス `DEST_FILE_AFTER_PATH` を指定します。",
		Destination: d,
	}
}

func opsOutput(o *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "output",
		Aliases:     []string{"o"},
		Usage:       "データを出力するファイルのパス `OUTPUT_FILE_PATH` を指定します。",
		Destination: o,
		Required:    true,
	}
}

func opsIgnore(g *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "ignore",
		Aliases:     []string{"g"},
		Usage:       "比較元ファイルのファイル名が `IGNORE` を含む場合、除外します。",
		Destination: g,
	}
}

func opsRecovery(r *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "recovery",
		Aliases:     []string{"r"},
		Usage:       "SPOへの再送信対象ファイルのリストののパス `RECOVERY_FILE_PATH` を指定します。",
		Destination: r,
		Required:    true,
	}
}

func opsSpopath(s *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "spopath",
		Aliases:     []string{"p"},
		Usage:       "SPOへアップロードパス `SPO_PATH` を指定します。",
		Destination: s,
	}
}

func opsTrimWord(t *string) *cli.StringFlag {
	return &cli.StringFlag{
		Name:        "trim",
		Aliases:     []string{"t"},
		Usage:       "SPOへのアップロードパスを生成する際に先頭から除外する `TRIM WORD` を指定します。",
		Destination: t,
	}
}

func generateDestMapFromTempFileListPath(pathBefore, pathAfter string) (map[string]*SizeAndDateModified, error) {
	// チェック先(処理前)のファイル
	destBeFp, err := os.Open(pathBefore)
	if err != nil {
		return nil, err
	}
	defer destBeFp.Close()

	// チェック先ファイルからチェック用のハッシュマップを生成する
	destMap, err := generateDestMapFromTempFileList(destBeFp)
	if err != nil {
		return nil, err
	}

	// チェック先(処理後)のファイル
	if pathAfter != "" {
		var destOldFp *os.File
		destOldFp, err := os.Open(pathAfter)
		if err != nil {
			return nil, err
		}
		defer destOldFp.Close()

		destMap, err = updateDestMapFromTempFileList(destMap, destOldFp)
		if err != nil {
			return nil, err
		}
	}

	return destMap, nil
}

func generateDestMapFromSPOFileListPath(path, prefix, sd string) (map[string]*SizeAndDateModified, error) {
	// チェック先のファイル
	destFp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer destFp.Close()

	// チェック先ファイルからチェック用のハッシュマップを生成する
	destMap, err := generateDestMapFromSPOFileList(destFp, prefix, sd)
	if err != nil {
		return nil, err
	}

	return destMap, nil
}

// NUM_CONCURRENT が未指定の場合は、CPU数の半分とする。
func getNumConcrent(n int) int {
	if n > 0 {
		return n
	}
	return runtime.NumCPU() / 2
}
