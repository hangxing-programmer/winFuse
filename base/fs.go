package base

import (
	"context"
	"fmt"
	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"winFuse/global"
	"winFuse/meta"
)

type MS struct {
	fuse.FileSystemBase
	Client     *minio.Client
	Bucket     string
	CacheFile  *os.File
	Lock       sync.Mutex
	MountPoint string
}

func initMinioClient() *minio.Client {
	minioClient, err := minio.New(global.GlobalSetting.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(global.GlobalSetting.AccessKey, global.GlobalSetting.SecretKey, ""),
		Secure: global.GlobalSetting.UseSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return minioClient
}
func NewClient() *minio.Client {
	return initMinioClient()
}
func NewMount() {
	client := NewClient()
	fs := &MS{Client: client, Bucket: global.GlobalSetting.BucketName, MountPoint: global.GlobalSetting.MountPoint}
	go fs.ScanEmptyDirAuto(global.GlobalSetting.BucketName, context.Background())
	//go autoRM()
	host := fuse.NewFileSystemHost(fs)
	str := []string{
		"-o", "allow_other",
		"-o", "name=minions",
		"-o", "uid=-1", // 设置匿名用户
		"-o", "gid=-1", // 设置匿名组
		"-o", "default_permissions", // 启用权限检查
		"-o", "attr_timeout=3", // 缓存时间3s
		"-o", "rw",
	}
	host.Mount(global.GlobalSetting.MountPoint, str)
}
func (fs *MS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	if path == "/" {
		stat.Mode = fuse.S_IFDIR | 0755
		return 0
	}

	ctx := context.Background()
	objectName := strings.TrimPrefix(path, "/")

	// 检查是否是目录（以/结尾的对象）
	if strings.HasSuffix(objectName, "/") {
		stat.Mode = fuse.S_IFDIR | 0755
		return 0
	}

	// 检查文件是否存在
	objInfo, err := fs.Client.StatObject(ctx, fs.Bucket, objectName, minio.StatObjectOptions{})
	if err == nil {
		stat.Mode = fuse.S_IFREG | 0644
		stat.Size = objInfo.Size
		return 0
	}

	// 检查是否是目录（包含子对象）
	ch := fs.Client.ListObjects(ctx, fs.Bucket, minio.ListObjectsOptions{
		Prefix:  objectName + "/",
		MaxKeys: 1,
	})
	if _, ok := <-ch; ok {
		stat.Mode = fuse.S_IFDIR | 0755
		return 0
	}

	return -fuse.ENOENT
}
func (fs *MS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	ctx := context.Background()
	prefix := ""
	if path != "/" {
		prefix = strings.TrimPrefix(path, "/") + "/"
	}
	// 添加当前目录（.）和父目录（..）
	fill(".", nil, 0)
	if path != "/" {
		fill("..", nil, 0)
	}
	// 获取目录和文件列表
	objectsCh := fs.Client.ListObjects(ctx, fs.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	// 用于标记已经处理过的路径
	processed := make(map[string]bool)

	for obj := range objectsCh {
		if obj.Err != nil {
			global.GlobalLogger.Println("Error listing objects:", obj.Err)
			continue
		}
		// 获取对象的完整路径
		fullPath := obj.Key
		// 检查是否是目录
		isDir := strings.HasSuffix(fullPath, "/")
		// 获取相对于当前路径的名称
		relativeName := strings.TrimPrefix(fullPath, prefix)
		if isDir {
			relativeName = strings.TrimSuffix(relativeName, "/")
		}
		// 去重
		if processed[relativeName] {
			continue
		}
		// 添加目录或文件
		var stat *fuse.Stat_t
		if isDir {
			stat = &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755}
		} else {
			stat = &fuse.Stat_t{Mode: fuse.S_IFREG | 0644, Size: obj.Size}
		}
		fill(relativeName, stat, 0)
		// 标记
		processed[relativeName] = true
	}

	return 0
}
func (fs *MS) Create(path string, flags int, mode uint32) (num int, fh uint64) {
	ctx := context.Background()
	objectName := strings.TrimPrefix(path, "/")

	// 检查文件是否已经存在
	_, err := fs.Client.StatObject(ctx, fs.Bucket, objectName, minio.StatObjectOptions{})
	if err == nil {
		// 文件已经存在
		return -fuse.EEXIST, 0
	}

	// 创建一个空文件（上传一个空对象）
	_, err = fs.Client.PutObject(ctx, fs.Bucket, objectName, strings.NewReader(""), 0, minio.PutObjectOptions{})
	if err != nil {
		global.GlobalLogger.Println("Error creating file:", err)
		return -fuse.EIO, 0
	}

	return 0, 0
}
func (fs *MS) Mkdir(path string, mode uint32) int {
	ctx := context.Background()
	dirName := strings.TrimPrefix(path, "/") + "/"

	// 检查目录是否已经存在
	_, err := fs.Client.StatObject(ctx, fs.Bucket, dirName, minio.StatObjectOptions{})
	if err == nil {
		// 目录已经存在
		return -fuse.EEXIST
	}

	// 创建目录（通过创建一个以 '/' 结尾的对象）
	_, err = fs.Client.PutObject(ctx, fs.Bucket, dirName, nil, 0, minio.PutObjectOptions{})
	if err != nil {
		global.GlobalLogger.Println("Error creating directory:", err)
		return -fuse.EIO
	}

	return 0
}
func (fs *MS) Unlink(path string) int {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()
	ctx := context.Background()
	objectName := strings.TrimPrefix(path, "/")

	// 删除对象
	err := fs.Client.RemoveObject(ctx, fs.Bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		global.GlobalLogger.Println("Error deleting file:", err)
		return -fuse.EIO
	}

	return 0
}
func (fs *MS) Rmdir(path string) int {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()
	ctx := context.Background()
	dirName := strings.TrimPrefix(path, "/")

	fmt.Println("dirName = ", dirName)

	err := meta.GlobalTikv.ExecuteTxn(func(txn *transaction.KVTxn) error {
		pathKey := meta.GetPathKey(global.GlobalSetting.TenantID, global.GlobalSetting.BucketName)
		num, err := txn.Get(ctx, pathKey)
		if err != nil {
			return err
		}
		objKey := meta.GetObjKey(global.GlobalSetting.TenantID, string(num), dirName)
		err = txn.Delete(objKey)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		global.GlobalLogger.Printf("Error deleting meta,file : %s, err : %v", dirName, err)
	}

	return 0
}
func (fs *MS) Open(path string, flags int) (errc int, fh uint64) {
	ctx := context.Background()
	objectName := strings.TrimPrefix(path, "/")

	_, err := fs.Client.StatObject(ctx, fs.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return -fuse.ENOENT, 0
		}

		global.GlobalLogger.Printf("Error checking if file exists: %v", err)
		return -fuse.EIO, 0
	}
	if flags&os.O_WRONLY != 0 || flags&os.O_RDWR != 0 {
		return -fuse.EACCES, 0
	}

	return 0, 0
}
func (fs *MS) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()

	objPath := strings.TrimPrefix(path, fs.MountPoint)
	obj, err := fs.Client.GetObject(context.Background(), fs.Bucket, objPath, minio.GetObjectOptions{})
	if err != nil {
		global.GlobalLogger.Printf("Failed to read object: %v", err)
		return -fuse.EIO
	}
	defer obj.Close()

	if _, err = obj.Seek(ofst, io.SeekStart); err != nil {
		global.GlobalLogger.Printf("Seek failed: %v", err)
		return -fuse.EIO
	}

	if ofst == 0 {
		return -fuse.EIO
	}
	n, err = obj.ReadAt(buff, ofst)
	if err != nil && err != io.EOF {
		global.GlobalLogger.Printf("Read failed: %v", err)
		return -fuse.EIO
	}
	return n
}
func (fs *MS) Release(path string, fh uint64) int {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()
	ctx := context.Background()
	objectName := strings.TrimPrefix(path, "/")
	file, _ := os.OpenFile(fs.CacheFile.Name(), os.O_RDWR, 0644)
	stat, _ := file.Stat()
	_, err := fs.Client.PutObject(ctx, fs.Bucket, objectName, file, stat.Size(), minio.PutObjectOptions{})
	if err != nil {
		global.GlobalLogger.Println("Error writing to file:", err)
		return -fuse.EIO
	}
	file.Close()

	err = os.Remove(fs.CacheFile.Name())
	if err != nil {
		global.GlobalLogger.Println("Error deleting cache file:", err)
		return -fuse.EIO
	}

	return 0
}
func (fs *MS) Write(path string, buff []byte, ofst int64, fh uint64) (n int) {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()
	fs.CacheFile, _ = os.OpenFile(global.GlobalSetting.Cache+"\\"+strings.TrimPrefix(path, "/"), os.O_CREATE|os.O_RDWR, 0644)
	ctx := context.Background()
	object, err := fs.Client.GetObject(ctx, fs.Bucket, path, minio.GetObjectOptions{})
	if err != nil {
		global.GlobalLogger.Println("Error getting object:", err)
		return -fuse.EIO
	}
	defer object.Close()
	_, err = io.Copy(fs.CacheFile, object)
	if err != nil {
		global.GlobalLogger.Printf("Failed to write to temporary file: %v", err)
		return -fuse.EIO
	}
	if ofst > 0 {
		fs.CacheFile.Close()
		err := fs.insertAtPosition(global.GlobalSetting.Cache+"\\"+strings.TrimPrefix(path, "/"), ofst, buff)
		if err != nil {
			global.GlobalLogger.Printf("Failed to insertAtPosition: %v", err)
		}
	} else {
		fs.CacheFile.Write(buff)
		fs.CacheFile.Close()
	}
	return len(buff)
}
func (fs *MS) Rename(oldpath string, newpath string) int {
	fs.Lock.Lock()
	defer fs.Lock.Unlock()
	ctx := context.Background()
	srcName := strings.TrimPrefix(oldpath, "/")
	dstName := strings.TrimPrefix(newpath, "/")

	// 排除非法字符
	if strings.ContainsAny(dstName, "\\:*?\"<>|") {
		return -fuse.EINVAL
	}

	// 目录判断
	isDir := strings.HasSuffix(srcName, "/") || fs.isDirectory(srcName)
	if isDir {
		if !strings.HasSuffix(srcName, "/") {
			srcName += "/"
		}
		if !strings.HasSuffix(dstName, "/") {
			dstName += "/"
		}
		return fs.renameDirectory(ctx, srcName, dstName)
	}

	if _, err := fs.Client.StatObject(ctx, fs.Bucket, dstName, minio.StatObjectOptions{}); err == nil {
		return -fuse.EEXIST
	}

	return fs.renameFile(ctx, srcName, dstName)
}
func (fs *MS) renameDirectory(ctx context.Context, srcDir, dstDir string) int {
	// 确保目标目录格式正确
	if !strings.HasSuffix(srcDir, "/") {
		srcDir += "/"
	}
	if !strings.HasSuffix(dstDir, "/") {
		dstDir += "/"
	}

	// 复制所有子对象
	objectsCh := fs.Client.ListObjects(ctx, fs.Bucket, minio.ListObjectsOptions{
		Prefix:    srcDir,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			continue
		}

		// 构建新对象名
		newKey := dstDir + strings.TrimPrefix(obj.Key, srcDir)

		// 复制对象
		srcOpts := minio.CopySrcOptions{Bucket: fs.Bucket, Object: obj.Key}
		dstOpts := minio.CopyDestOptions{Bucket: fs.Bucket, Object: newKey}
		if _, err := fs.Client.CopyObject(ctx, dstOpts, srcOpts); err != nil {
			global.GlobalLogger.Printf("Copy failed: %s -> %s : %v", obj.Key, newKey, err)
			return -fuse.EIO
		}
	}

	// 删除原目录及其内容
	objectsCh = fs.Client.ListObjects(ctx, fs.Bucket, minio.ListObjectsOptions{
		Prefix:    srcDir,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			continue
		}
		if err := fs.Client.RemoveObject(ctx, fs.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			global.GlobalLogger.Printf("Delete failed: %s : %v", obj.Key, err)
			return -fuse.EIO
		}
	}
	return 0
}
func (fs *MS) isDirectory(path string) bool {
	ctx := context.Background()

	// 同时检查两种目录表示方式
	if strings.HasSuffix(path, "/") {
		_, err := fs.Client.StatObject(ctx, fs.Bucket, path, minio.StatObjectOptions{})
		return err == nil
	}

	ch := fs.Client.ListObjects(ctx, fs.Bucket, minio.ListObjectsOptions{
		Prefix:  path + "/",
		MaxKeys: 1,
	})
	_, ok := <-ch
	return ok
}
func (fs *MS) renameFile(ctx context.Context, src, dst string) int {

	if _, err := fs.Client.StatObject(ctx, fs.Bucket, src, minio.StatObjectOptions{}); err != nil {
		return -fuse.ENOENT
	}

	_, err := fs.Client.ComposeObject(ctx, minio.CopyDestOptions{
		Bucket: fs.Bucket,
		Object: dst,
	}, minio.CopySrcOptions{
		Bucket: fs.Bucket,
		Object: src,
	})
	if err != nil {
		global.GlobalLogger.Printf("Copy failed: %v", err)
		return -fuse.EIO
	}

	defer func() {
		if err := fs.Client.RemoveObject(ctx, fs.Bucket, src, minio.RemoveObjectOptions{}); err != nil {
			global.GlobalLogger.Printf("Warning: delete source failed: %v", err)
		}
	}()

	return 0
}
func (fs *MS) insertAtPosition(srcPath string, offset int64, insertData []byte) error {
	// 打开原文件
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer srcFile.Close()

	// 创建临时文件
	tmpFile, err := os.CreateTemp(filepath.Dir(srcPath), "insert-*.tmp")
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// 复制前半部分（0~offset字节）
	if _, err = io.CopyN(tmpFile, srcFile, offset); err != nil && err != io.EOF {
		return fmt.Errorf("复制前半部分失败: %w", err)
	}

	// 写入数据
	if _, err = tmpFile.Write(insertData); err != nil {
		return fmt.Errorf("写入插入数据失败: %w", err)
	}

	// 复制后半部分（从offset开始）
	if _, err = srcFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("定位原文件偏移失败: %w", err)
	}
	if _, err = io.Copy(tmpFile, srcFile); err != nil && err != io.EOF {
		return fmt.Errorf("复制后半部分失败: %w", err)
	}

	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("关闭临时文件失败: %w", err)
	}
	if err = srcFile.Close(); err != nil {
		return fmt.Errorf("关闭原文件失败: %w", err)
	}

	if runtime.GOOS == "windows" {
		if err = os.Remove(srcPath); err != nil {
			return fmt.Errorf("删除原文件失败: %w", err)
		}
	}
	if err = os.Rename(tmpPath, srcPath); err != nil {
		return fmt.Errorf("重命名文件失败: %w", err)
	}

	return nil
}
func (fs *MS) ScanEmptyDirAuto(bucket string, ctx context.Context) {
	ticker := time.NewTicker(6 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			objects := fs.Client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
				Recursive: true,
			})
			for object := range objects {
				if object.Err != nil {
					global.GlobalLogger.Printf("ScanEmptyDirAuto err:%v", object.Err)
					continue
				}
				if strings.HasSuffix(object.Key, "/") {
					err := meta.GlobalTikv.RmdirAuto(object.Key)
					if err != nil && !strings.Contains(err.Error(), "not exist") {
						global.GlobalLogger.Printf("error to remove empty dir(%s),err:%v", object.Key, err)
					}
				}
			}
		}
	}
}
