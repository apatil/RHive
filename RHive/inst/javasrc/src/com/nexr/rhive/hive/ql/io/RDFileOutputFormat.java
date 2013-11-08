package com.nexr.rhive.hive.ql.io;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;


public class RDFileOutputFormat extends FileOutputFormat<WritableComparable, BytesRefArrayWritable> implements HiveOutputFormat<WritableComparable, BytesRefArrayWritable> {

	@Override
	public RecordWriter getRecordWriter(FileSystem ignored, JobConf jobConf, String name, Progressable progress) throws IOException {
		Path outputPath = getWorkOutputPath(jobConf);
		FileSystem fs = outputPath.getFileSystem(jobConf);

		Path file = new Path(outputPath, name);
		
		CompressionCodec codec = null;
		if (getCompressOutput(jobConf)) {
			Class<?> codecClass = getOutputCompressorClass(jobConf, DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jobConf);
		}
		
		final RDFile.Writer out = new RDFile.Writer(fs, jobConf, file, codec, progress);

		return new RecordWriter<WritableComparable, BytesRefArrayWritable>() {

			@Override
			public void close(Reporter reporter) throws IOException {
				out.close();
			}

			@Override
			public void write(WritableComparable key, BytesRefArrayWritable value) throws IOException {
				out.append(value);
			}
		};
	}

	@Override
	public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path outputPath, Class valueClass, boolean isCompressed,
			Properties tableProperties, Progressable progress) throws IOException {
		
		String[] cols = null;
		String columns = tableProperties.getProperty("columns");
		if (columns == null || columns.trim().equals("")) {
			cols = new String[0];
		} else {
			cols = StringUtils.split(columns, ",");
		}

		RDFile.setColumnNumber(jobConf, cols.length);
		RDFile.setColumnNames(jobConf, columns);
		
		String columnTypes = tableProperties.getProperty("columns.types");
		RDFile.setColumnTypes(jobConf, columnTypes);
		
		String name = tableProperties.getProperty("data.name");
		if (name != null && !name.trim().equals("")) {
			RDFile.setDataName(jobConf, name);
		}
		
		String s = tableProperties.getProperty("buffer.size");
		if (s != null) {
			RDFile.setBufferSize(jobConf, Integer.parseInt(s));
		}

		s = tableProperties.getProperty("group.size");
		if (s != null) {
			RDFile.setGroupSize(jobConf, Integer.parseInt(s));
		}
		
		final RDFile.Writer writer = createRDFileWriter(jobConf, outputPath.getFileSystem(jobConf), outputPath, isCompressed);

		return new FileSinkOperator.RecordWriter() {

			public void write(Writable r) throws IOException {
				writer.append(r);
			}

			public void close(boolean abort) throws IOException {
				writer.close();
			}
		};
	}

	public static RDFile.Writer createRDFileWriter(JobConf jobConf, FileSystem fs, Path file, boolean isCompressed) throws IOException {
		CompressionCodec codec = null;
		Class codecClass = null;
		if (isCompressed) {
			codecClass = FileOutputFormat.getOutputCompressorClass(jobConf, DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jobConf);
		}
		
		return new RDFile.Writer(fs, jobConf, file, codec);
	}
}