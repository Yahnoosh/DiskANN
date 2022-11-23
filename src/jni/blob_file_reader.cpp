#include <jni.h>
#include "jni/blob_file_reader.h"

#include <iostream>

BlobFileReader::BlobFileReader(JNIEnv &env, const std::string &storageAccount, const std::string &blobContainerName, 
  const std::string &cacheDir, long cacheSizeBytes, int fragmentSize) : 
    env(env), storageAccount(storageAccount), blobContainerName(blobContainerName),
    cacheDir(cacheDir), cacheSizeBytes(cacheSizeBytes), fragmentSize(fragmentSize) {
  this->javaClass = this->env.FindClass("com/microsoft/azure/search/serverlessserver/storage/DiskAnnFileInputStreamAdapter");
  if (this->javaClass == nullptr) {
    std::cerr << "ERROR: Java class com.microsoft.azure.search.serverlessserver.storage.DiskAnnFileInputStreamAdapter not found" << std::endl;
  }
  this->javaConstructor = this->env.GetMethodID(this->javaClass, "<init>", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;JI)V");
  // this would be a great place to verify the methods and arguments we expect to pass in are meeting our criteria
  if (this->javaConstructor == nullptr) {
    std::cerr << "ERROR: Java class DiskAnnFileInputStreamAdapter does not comply with expected interface (ctor)" << std::endl;
  }
  this->javaClose = this->env.GetMethodID(this->javaClass, "close", "()V");
  if (this->javaClose == nullptr) {
    std::cerr << "ERROR: Java class DiskAnnFileInputStreamAdapter does not comply with expected interface (close)" << std::endl;
  }
  // https://docs.oracle.com/javase/1.5.0/docs/guide/jni/spec/types.html#wp276 you'll need this. trust me.
  this->javaRead = this->env.GetMethodID(this->javaClass, "read", "(Ljava/nio/ByteBuffer;II)V");
  if (this->javaRead == nullptr) {
    std::cerr << "ERROR: Java class DiskAnnFileInputStreamAdapter does not comply with expected interface (read)" << std::endl;
  }
}

BlobFileReader::~BlobFileReader() {
   // I do nothing
}

void BlobFileReader::open(const std::string &fname) {
  jobject fileName = this->env.NewStringUTF(fname.c_str());
  jobject storageAccount = this->env.NewStringUTF(this->storageAccount.c_str());
  jobject blobContainerName = this->env.NewStringUTF(this->blobContainerName.c_str());
  jobject cacheDir = this->env.NewStringUTF(this->cacheDir.c_str());
  this->blobFileReader = this->env.NewObject(this->javaClass, this->javaConstructor, storageAccount, blobContainerName,
    fileName, cacheDir, this->cacheSizeBytes, this->fragmentSize);
}

void BlobFileReader::close() {
  this->env.CallVoidMethod(this->blobFileReader, this->javaClose);
  if (this->env.ExceptionOccurred()) {
    std::cerr << "Things went wrong when we closed our JNI File Reader: " << std::endl;
    this->env.ExceptionDescribe();
    this->env.ExceptionClear();
    exit(-1);
  }
}

void BlobFileReader::read(std::vector<AlignedRead> &read_reqs, IOContext& ctx, bool async) {
  for (size_t i = 0; i < read_reqs.size(); i++) {
    AlignedRead aligned = read_reqs[i];
    // TODO: Can we avoid allocating a direct buffer for each read, can the caller declare buffers that can be reused?
    jobject dst = this->env.NewDirectByteBuffer(aligned.buf, aligned.len); 
    this->env.CallVoidMethod(this->blobFileReader, this->javaRead, dst, (int)aligned.offset, (int)aligned.len);
  }
}