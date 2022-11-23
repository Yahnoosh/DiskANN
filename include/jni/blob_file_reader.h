#include <jni.h>
#include "aligned_file_reader.h"

class BlobFileReader : public AlignedFileReader { 
 private:
  JNIEnv &env;

  jclass javaClass;
  jmethodID javaConstructor;
  jmethodID javaRead;
  jmethodID javaClose; 
  jobject blobFileReader;

  IOContext ctx;

  const std::string &storageAccount; 
  const std::string &blobContainerName;
  const std::string &cacheDir;
  const long cacheSizeBytes;
  const int fragmentSize;

 public: 
  BlobFileReader(JNIEnv& env, const std::string &storageAccount, const std::string &blobContainerName,
   const std::string &cacheDir, long cacheSizeBytes, int fragmentSize);

  ~BlobFileReader();

  static bool InitializeJVM(JavaVM **jvm, JNIEnv **env, const std::string &jarPath) {
    JavaVMInitArgs vm_args;
    JavaVMOption* options = new JavaVMOption[1];
    std::string option = "-Djava.class.path=" + jarPath;
    options[0].optionString = const_cast<char*>(option.c_str());
    vm_args.version = JNI_VERSION_10; // OpenJDK 19 offers version _19 but in OpenJDK 18 _10 is the latest
    vm_args.nOptions = 1;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = false;
    
    jint rc = JNI_CreateJavaVM(jvm, (void**)env, &vm_args);
    delete options;
    return rc == JNI_OK;
  }

  virtual IOContext& get_ctx() { return ctx; };

  // register thread-id for a context
  void register_thread() {};
  // de-register thread-id for a context
  void deregister_thread() {};
  void deregister_all_threads() {};

  void open(const std::string &fname);
  void close();

  void read(std::vector<AlignedRead> &read_reqs, IOContext& ctx, bool async = false);
};
