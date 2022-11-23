#include <jni.h>
#include <iostream>
#include "jni/blob_file_reader.h"
#include "aligned_file_reader.h"

int main()
{
  JavaVM *jvm;
  JNIEnv *env;
  std::string indexServerJarPath = "/home/jlembicz/LuceneIndexServer/build/libs/serverlessserver-1.0-SNAPSHOT.jar";
  
   if (!BlobFileReader::InitializeJVM(&jvm, &env, indexServerJarPath)) {
    std::cerr << "Something went wrong creating the JavaVM for DiskANN!" << std::endl;
    return 0;
  }
  std::cout << "Created JVM" << std::endl; 
  
  std::vector<AlignedRead> reads;
  AlignedRead aligned;
  aligned.offset = 0;
  aligned.len = 10*1024;

  aligned.buf = malloc(aligned.len); 
  reads.push_back(aligned);

  std::string storageAccount = "jldiskanntest"; 
  std::string blobContainerName = "diskann";
  std::string cacheDir = "/home/jlembicz/github/diskann/localcache";
  long cacheSizeBytes = 1024*1024*1024;
  int fragmentSize = 4*1024*1024;

  BlobFileReader reader(*env, storageAccount, blobContainerName, cacheDir, cacheSizeBytes, fragmentSize);
  reader.open("CMakeLists.txt");
  
  IOContext io_context;
  reader.read(reads, io_context, false);

  
  std::string s((char*)aligned.buf, aligned.len);
  std::cout << "Read string: " << s << std::endl;

  std::cin.ignore();
  std::cout << "Destroying JVM" << std::endl;
  jvm->DestroyJavaVM(); 
}
