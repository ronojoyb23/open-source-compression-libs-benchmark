/* Compression Libraries Benchmarking, @author - Ronojoy Bhaumik*/
package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func main() {
	analyze_gzip_performance()
	analyze_zlib_performance()
	analyze_snappy_performance()
	analyze_ZSTD_performance()
}
func analyze_gzip_performance() {
	//Zip operation
	unzipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unzipped/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToZip := time.Now()
	var size_before_compress int64
	size_before_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unzipped/")
	for _, fileInfo := range unzipped_files {

		//fmt.Println(fileInfo.Name())
		f, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unzipped/" + fileInfo.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		name := fileInfo.Name()
		create_Gzip(f, name)
	}
	var size_after_compress int64
	size_after_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-zipped/")
	duration_to_zip := time.Since(startTimeToZip)
	log.Println("-----------------------------  GZIP RESULTS -----------------------------")
	log.Println("# of files : ", len(unzipped_files))
	log.Println("Total File Size before Compression : ", size_before_compress, " bytes")
	log.Println("Total File Size after Compression : ", size_after_compress, " bytes")
	diff := float64(size_after_compress - size_before_compress)
	delta := (diff / float64(size_before_compress)) * 100
	log.Println("Compression Ratio : ", delta, "%")
	log.Println("Duration to zip: ", duration_to_zip)

	// Unzip operation

	zipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-zipped/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToUnzip := time.Now()
	//allFilesContent := make([][]byte, len(zipped_files))

	for _, fileInfo_zipped := range zipped_files {
		//fmt.Println(fileInfo.Name())
		file_zipped, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-zipped/" + fileInfo_zipped.Name())
		//fmt.Println("File info :" , fileInfo_zipped.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		length := open_Gzip(file_zipped)
		if false {
			fmt.Println("Returned from open_Gzip data length is:", length)
		}

	}
	duration_to_unzip := time.Since(startTimeToUnzip)
	log.Println("Duration to unzip: ", duration_to_unzip)
	log.Println("---------------------------------------------------------------------------")
}
func open_Gzip(f *os.File) int {
	gr, err_1 := gzip.NewReader(f)
	if err_1 != nil {
		log.Fatalln("Cannot unzip GZip. ", err_1)
	}
	data, err_2 := ioutil.ReadAll(gr)
	if err_2 != nil {
		log.Fatalln("Cannot read bytes. ", err_2)
	}
	gr.Close()
	return len(data)
}
func create_Gzip(f *os.File, name string) {

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read raw protobuf. ", err)
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(data)
	w.Close()
	//fmt.Println("f.Name() is : " + f.Name())
	s := "/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-zipped/" + name + ".gz"
	err2 := ioutil.WriteFile(s, b.Bytes(), 0644)
	if err2 != nil {
		log.Fatal(err)
	}
}
func open_zlib(f *os.File) int {
	gr, err_1 := zlib.NewReader(f)
	if err_1 != nil {
		log.Fatalln("Cannot unzip. ", err_1)
	}
	data, err_2 := ioutil.ReadAll(gr)
	if err_2 != nil {
		log.Fatalln("Cannot read bytes. ", err_2)
	}
	gr.Close()
	return len(data)
}
func create_zlib(f *os.File, name string) {

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read raw protobuf. ", err)
	}
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(data)
	w.Close()
	//fmt.Println("f.Name() is : " + f.Name())
	s := "/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZLIB/" + name + ".gz"
	err2 := ioutil.WriteFile(s, b.Bytes(), 0644)
	if err2 != nil {
		log.Fatal(err)
	}
}
func analyze_zlib_performance() {
	//Zip operation
	unzipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToZip := time.Now()
	var size_before_compress int64
	size_before_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	for _, fileInfo := range unzipped_files {

		//fmt.Println(fileInfo.Name())
		f, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/" + fileInfo.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		name := fileInfo.Name()
		create_zlib(f, name)
	}
	var size_after_compress int64
	size_after_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZLIB/")
	duration_to_zip := time.Since(startTimeToZip)
	log.Println("-----------------------------  ZLIB RESULTS -------------------------------")
	log.Println("# of files : ", len(unzipped_files))
	log.Println("Total File Size before Compression : ", size_before_compress, " bytes")
	log.Println("Total File Size after Compression : ", size_after_compress, " bytes")
	diff := float64(size_after_compress - size_before_compress)
	delta := (diff / float64(size_before_compress)) * 100
	log.Println("Compression Ratio : ", delta, "%")
	log.Println("Duration to zip: ", duration_to_zip)

	// Unzip operation

	zipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZLIB/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToUnzip := time.Now()
	//allFilesContent := make([][]byte, len(zipped_files))

	for _, fileInfo_zipped := range zipped_files {
		//fmt.Println(fileInfo.Name())
		file_zipped, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZLIB/" + fileInfo_zipped.Name())
		//fmt.Println("File info :" , fileInfo_zipped.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		length := open_zlib(file_zipped)
		if false {
			fmt.Println("Returned from open_Gzip data length is:", length)
		}

	}
	duration_to_unzip := time.Since(startTimeToUnzip)
	log.Println("Duration to unzip: ", duration_to_unzip)
	log.Println("---------------------------------------------------------------------------")
}
func calculateDirSize(dirpath string) (dirsize int64, err error) {
	err = os.Chdir(dirpath)
	if err != nil {
		return
	}
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return
	}

	for _, file := range files {
		if file.Mode().IsRegular() {
			dirsize += file.Size()
		}
	}
	return
}
func open_snappy(f *os.File) int {
	var gr *snappy.Reader
	gr = snappy.NewReader(f)
	data, err_2 := ioutil.ReadAll(gr)
	if err_2 != nil {
		log.Fatalln("Cannot read bytes. ", err_2)
	}
	return len(data)
}
func create_snappy(f *os.File, name string) {

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read raw protobuf. ", err)
	}
	var b bytes.Buffer
	w := snappy.NewWriter(&b)
	//fmt.Println("data byte[] size in snappy: ", len(data))
	w.Write(data)
	w.Close()
	//fmt.Println("f.Name() is : " + f.Name())
	s := "/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-snappy/" + name + ".snappy"
	err2 := ioutil.WriteFile(s, b.Bytes(), 0644)
	if err2 != nil {
		log.Fatal(err)
	}
}
func analyze_snappy_performance() {
	//Zip operation
	unzipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToZip := time.Now()
	var size_before_compress int64
	size_before_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	for _, fileInfo := range unzipped_files {

		//fmt.Println(fileInfo.Name())
		f, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/" + fileInfo.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		name := fileInfo.Name()
		create_snappy(f, name)
	}
	var size_after_compress int64
	size_after_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-snappy/")
	duration_to_zip := time.Since(startTimeToZip)
	log.Println("-----------------------------  SNAPPY RESULTS -----------------------------")
	log.Println("# of files : ", len(unzipped_files))
	log.Println("Total File Size before Compression : ", size_before_compress, " bytes")
	log.Println("Total File Size after Compression : ", size_after_compress, " bytes")
	diff := float64(size_after_compress - size_before_compress)
	delta := (diff / float64(size_before_compress)) * 100
	log.Println("Compression Ratio : ", delta, "%")
	log.Println("Duration to zip: ", duration_to_zip)

	// Unzip operation

	zipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-snappy/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToUnzip := time.Now()
	//allFilesContent := make([][]byte, len(zipped_files))

	for _, fileInfo_zipped := range zipped_files {
		//fmt.Println(fileInfo.Name())
		file_zipped, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-snappy/" + fileInfo_zipped.Name())
		//fmt.Println("File info :" , fileInfo_zipped.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		length := open_snappy(file_zipped)
		if false {
			fmt.Println("Returned from open_Gzip data length is:", length)
		}

	}
	duration_to_unzip := time.Since(startTimeToUnzip)
	log.Println("Duration to unzip: ", duration_to_unzip)
	log.Println("---------------------------------------------------------------------------")
}
func open_ZSTD(f *os.File) int {
	gr, err := zstd.NewReader(f)
	if err != nil {
		log.Fatalln("zstd new reader exception. ", err)
	}
	data, err_2 := ioutil.ReadAll(gr)
	if err_2 != nil {
		log.Fatalln("Cannot read bytes. ", err_2)
	}
	return len(data)
}
func create_ZSTD(f *os.File, name string) {

	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read raw protobuf. ", err)
	}
	var b bytes.Buffer

	w, err := zstd.NewWriter(&b)
	if err != nil {
		log.Fatalln("Cannot create new zstd writer. ", err)
	}
	//fmt.Println("data byte[] size in zstd: ", len(data))
	w.Write(data)
	w.Close()
	//fmt.Println("f.Name() is : " + f.Name())
	s := "/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZSTD/" + name + ".zst"
	err2 := ioutil.WriteFile(s, b.Bytes(), 0644)
	if err2 != nil {
		log.Fatal(err)
	}
}
func analyze_ZSTD_performance() {
	//Zip operation
	unzipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToZip := time.Now()
	var size_before_compress int64
	size_before_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/")
	for _, fileInfo := range unzipped_files {

		//fmt.Println(fileInfo.Name())
		f, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-unZLIB/" + fileInfo.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		name := fileInfo.Name()
		create_ZSTD(f, name)
	}
	var size_after_compress int64
	size_after_compress, err = calculateDirSize("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZSTD/")
	duration_to_zip := time.Since(startTimeToZip)
	log.Println("-----------------------------  ZSTD RESULTS -------------------------------")
	log.Println("# of files : ", len(unzipped_files))
	log.Println("Total File Size before Compression : ", size_before_compress, " bytes")
	log.Println("Total File Size after Compression : ", size_after_compress, " bytes")
	diff := float64(size_after_compress - size_before_compress)
	delta := (diff / float64(size_before_compress)) * 100
	log.Println("Compression Ratio : ", delta, "%")
	log.Println("Duration to zip: ", duration_to_zip)

	// Unzip operation

	zipped_files, err := ioutil.ReadDir("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZSTD/")
	if err != nil {
		log.Fatal(err)
	}

	startTimeToUnzip := time.Now()
	//allFilesContent := make([][]byte, len(zipped_files))

	for _, fileInfo_zipped := range zipped_files {
		//fmt.Println(fileInfo.Name())
		file_zipped, err := os.Open("/Users/ronojoy.bhaumik/Documents/codebase/vidataformatcomparison/proto-ZSTD/" + fileInfo_zipped.Name())
		//fmt.Println("File info :" , fileInfo_zipped.Name())
		if err != nil {
			log.Fatalln("Error reading file:", err)
		}
		length := open_ZSTD(file_zipped)
		if false {
			fmt.Println("Returned from open_Gzip data length is:", length)
		}

	}
	duration_to_unzip := time.Since(startTimeToUnzip)
	log.Println("Duration to unzip: ", duration_to_unzip)
	log.Println("-----------------------------------------------------------------------------")
}
