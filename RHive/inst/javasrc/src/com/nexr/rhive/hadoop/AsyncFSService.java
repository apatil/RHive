package com.nexr.rhive.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class AsyncFSService {
	private ExecutorService executor;
	
	public AsyncFSService(int nThreads) {
		this.executor = Executors.newFixedThreadPool(nThreads, Executors.defaultThreadFactory());
	}
	
	public void copyToLocal(String id, boolean delSrc, String src, String dst, String defaultFS, boolean cleanIfExists) throws IOException, InterruptedException, ExecutionException {
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);

		Configuration conf = getConf(defaultFS); 
		FileSystem fs = FileSystem.get(conf);

		ExecutorCompletionService<String> completionService = new ExecutorCompletionService<String>(executor);
		FileStatus fileStatus = fs.getFileStatus(srcPath);
		
		int runningTasks = 0;
		if (fileStatus.isDir()) {
			FileStatus[] fileStatuses = fs.listStatus(srcPath, new HiddenPathFilter());
			
			File dir = new File(String.format("%s%s%s", dst, File.separator, srcPath.getName()));
			if (dir.exists()) {
				if (cleanIfExists) {
					FileUtils.cleanDirectory(dir);
				}
			} else {
				FileUtils.forceMkdir(dir);
			}
			
			dstPath = new Path(dir.getAbsolutePath());
			for (int i = 0; i < fileStatuses.length; i++) {
				completionService.submit(new CopyToLocalTask(String.format("%s-%d", id, i),
						delSrc, fileStatuses[i].getPath(), dstPath, defaultFS));
				
				runningTasks++;
			}

		} else {
			completionService.submit(new CopyToLocalTask(id, delSrc, srcPath, dstPath, defaultFS));
			runningTasks++;
		}
		
		for (int i = runningTasks; i > 0; i--) {
			completionService.take();
		}

		closeFileSystem(fs);
	}
	
	static class CopyToLocalTask implements Callable<String> {
		private String id;
		private boolean delSrc;
		private Path src;
		private Path dst;
		private String defaultFS;
		

		CopyToLocalTask(String id, boolean delSrc, Path src, Path dst, String defaultFS) {
			this.id = id;
			this.delSrc = delSrc;
			this.src = src;
			this.dst = dst;
			this.defaultFS = defaultFS;
		}


		@Override
		public String call() throws Exception {
			Configuration conf = getConf(defaultFS); 
			
			FileSystem fs = null;
			fs = FileSystem.get(conf);
			fs.copyToLocalFile(delSrc, src, dst);
			
			return id;
		}
	}
	
	protected void finalize() {
		executor.shutdown();
	}
	
	
	private static void closeFileSystem(FileSystem fs) {
		try {
			if (fs != null) {
				fs.close();
			}
		} catch (IOException e) { }
	}
	
	
	private static Configuration getConf(String defaultFS) {
		Configuration conf = new Configuration();
		
		if (defaultFS != null) {
			FileSystem.setDefaultUri(conf, defaultFS);
		}
		
		return conf;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		AsyncFSService service = new AsyncFSService(3);
		service.copyToLocal("copy", false, "/rhive/data", "/home/alephomega", "hdfs://localhost:8020", true);
		service.executor.shutdown();
	}
}