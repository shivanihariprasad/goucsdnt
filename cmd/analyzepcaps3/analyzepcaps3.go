package main

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/CAIDA/goucsdnt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
)

type MiraiMap map[netip.Addr]int
type MiraiCount struct {
	Mmap  MiraiMap
	Mchan chan netip.Addr
	Wg    sync.WaitGroup
}

func main() {
	var outputdir, starttime, endtime string
	var startts, endts time.Time
	var cntmax int64
	var workers int
	var wgglob sync.WaitGroup
	flag.StringVar(&starttime, "s", "2025-01-25T00:00:00Z", "Start time")
	flag.StringVar(&endtime, "e", "2025-01-25T00:59:59Z", "End time")
	flag.StringVar(&outputdir, "o", "./output", "Output dir path")
	flag.Int64Var(&cntmax, "c", -1, "Packet count")
	flag.IntVar(&workers, "w", 1, "Number of workers")
	flag.Parse()
	ctx := context.Background()
	client := goucsdnt.NewUCSDNTBucket(ctx)
	if client == nil {
		log.Fatalln("NewUCSDNTBucket failed")
	}
	// fix the date for demo purpose
	if starttime != "" {
		startts, _ = time.Parse(time.RFC3339, starttime)
	} else {
		startts = time.Date(2025, time.January, 25, 0, 0, 0, 0, time.UTC)
	}
	if endtime != "" {
		endts, _ = time.Parse(time.RFC3339, endtime)
	} else {
		endts = time.Date(2025, time.January, 25, 0, 59, 59, 0, time.UTC)
	}
	//create output dir, if not exist
	if _, err := os.Stat(outputdir); os.IsNotExist(err) {
		os.Mkdir(outputdir, 0755)
	}
	workerchan := make(chan int, workers)

	for sts := startts; sts.Before(endts); sts = sts.Add(time.Hour) {
		log.Println("Processing", sts.String())
		workerchan <- 1
		wgglob.Add(1)
		go func(ts time.Time) {

			LoadPcap(client, ts, cntmax, outputdir)
			wgglob.Done()
			<-workerchan
		}(sts)
	}
	wgglob.Wait()
	log.Println("Done")
}

func LoadPcap(client *goucsdnt.UCSDNTBucket, curday time.Time, cntmax int64, outputdir string) {
	var pckcnt int64
	log.Println("Loading pcap for", curday.Unix())
	mairamap := &MiraiCount{}
	mairamap.Mmap = make(MiraiMap)
	mairamap.Mchan = make(chan netip.Addr)
	go SumMaira(mairamap)
	mairamap.Wg.Add(1)
	pckcnt = 0
	pcapname, pcapio, err := client.GetObjectByDatetime(curday)
	if err == nil {
		gzipReader, err := gzip.NewReader(pcapio)
		if err != nil {
			log.Fatal(err)
		}
		defer gzipReader.Close()

		// Process the decompressed data from gzipReader
		p, err := pcapgo.NewReader(gzipReader)
		if err != nil {
			log.Fatal(err)
		}
		for {
			pck, _, err := p.ReadPacketData()
			pckcnt++
			if err == io.EOF || (pckcnt > cntmax && cntmax > 0) {
				close(mairamap.Mchan)
				break
			} else if err != nil {
				log.Fatal(err)
			}
			// Process the packet
			packet := gopacket.NewPacket(pck, layers.LayerTypeEthernet, gopacket.NoCopy)
			CheckMairaPacket(packet, mairamap.Mchan)
		}
	} else {
		log.Fatal(err)
	}
	mairamap.Wg.Wait()
	PrintMaira(mairamap.Mmap, outputdir, pcapname)
}

func CheckMairaPacket(packet gopacket.Packet, mchan chan netip.Addr) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if ipLayer != nil && tcpLayer != nil {
		ip, _ := ipLayer.(*layers.IPv4)
		dstIP := binary.BigEndian.Uint32(ip.DstIP)
		tcp, _ := tcpLayer.(*layers.TCP)
		if dstIP == tcp.Seq { // Mirai
			if mip, ok := netip.AddrFromSlice(ip.SrcIP); ok {
				mchan <- mip
			}
		}
	}
	return
}

func SumMaira(mmap *MiraiCount) {
	// Read the pcapio and sum the number of packets
	// For example, you can use the pcap library to read the pcapio
	// and sum the number of packets
	for c := range mmap.Mchan {
		//log.Println("Mirai IP:", c)
		mmap.Mmap[c]++
	}
	mmap.Wg.Done()
	return
}

func PrintMaira(mmap MiraiMap, outputdir string, filename string) {
	outfilename := strings.TrimSuffix(filename, ".pcap.gz") + ".csv"
	file, err := os.Create(filepath.Join(outputdir, outfilename))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for k, v := range mmap {
		_, err := file.WriteString(fmt.Sprintf("%s,%d\n", k, v))
		if err != nil {
			log.Fatal(err)
		}
	}
	return
}
