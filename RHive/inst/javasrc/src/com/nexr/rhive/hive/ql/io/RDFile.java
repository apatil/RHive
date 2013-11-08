package com.nexr.rhive.hive.ql.io;

import hep.io.xdr.XDROutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.jdbc.Utils;
import org.apache.hadoop.hive.ql.io.CodecPool;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;

public class RDFile {
	private static final Log LOG = LogFactory.getLog(RCFile.class);

	static class Header {
		private String magic;
		private String format;
		private int serializationVersion;
		private int version;
		private int minimalVersion;

		String getMagic() {
			return magic;
		}

		void setMagic(String magic) {
			this.magic = magic;
		}

		String getFormat() {
			return format;
		}

		void setFormat(String format) {
			this.format = format;
		}

		int getSerializationVersion() {
			return serializationVersion;
		}

		void setSerializationVersion(int serializationVersion) {
			this.serializationVersion = serializationVersion;
		}

		int getVersion() {
			return version;
		}

		void setVersion(int version) {
			this.version = version;
		}

		int getMinimalVersion() {
			return minimalVersion;
		}

		void setMinimalVersion(int minimalVersion) {
			this.minimalVersion = minimalVersion;
		}
	}
	
	static class GP {
		private static final int CACHED_MASK = (0x1 << 5);
		private static final int HASHASH_MASK = 0x1;
		private static final int IS_OBJECT_BIT_MASK = (0x1 << 8);
		private static final int HAS_ATTR_BIT_MASK = (0x1 << 9);
		private static final int HAS_TAG_BIT_MASK = (0x1 << 10);
		
		private static final int LATIN1_BIT_MASK = (0x1 << 2);
		private static final int ASCII_BIT_MASK = (0x1 << 6);
		private static final int UTF8_BIT_MASK = (0x1 << 3);
		
		private static final int LCK_BIT_MASK = (0x1 << 14);

		private static final int NA_INTEGER = -2147483648;
		private static final int NA_REAL_HW = 0x7ff80000;
		private static final int NA_REAL_LW = 0x000007a2;
		
		private static final int NULL_FLAGS = 254;
		
		
		static int flags(Type type, int levels, boolean isObject, boolean hasAttr, boolean hasTag) {
			int flags;
			if (type == Type.CHAR) {
				// scalar string type, used for symbol names
				levels &= (~(CACHED_MASK | HASHASH_MASK));
			}

			flags = type.getValue() | (levels << 12);
			if (isObject) {
				flags |= IS_OBJECT_BIT_MASK;
			}
			if (hasAttr) {
				flags |= HAS_ATTR_BIT_MASK;
			}
			if (hasTag) {
				flags |= HAS_TAG_BIT_MASK;
			}

			return flags;
		}
		
		static int getEncodingBitMask(String encoding) {
			if (encoding != null) {
				if ("ascii".equals(encoding)) {
					return ASCII_BIT_MASK;
				} else if (encoding.matches("latin[-_]?1")) {
					return LATIN1_BIT_MASK;
				} else if (encoding.matches("utf[-_]?8")) {
					return UTF8_BIT_MASK;
				}
			}

			return ASCII_BIT_MASK;
		}
	}
	

	static enum Type {
		NIL(0),
		SYM(1),
		LIST(2),
		CHAR(9),
		LGC(10),
		INT(13),
		REAL(14),
		CPLX(15),
		STR(16),
		VEC(19);
		
		private int value;
		
		Type(int value) {
			this.value = value;
		}
		
		int getValue() {
			return value;
		}
	}
	
	public static class Writer {
		private Configuration conf;
		private XDROutputStream out;
		private Metadata metadata = null;
		
		private String name;

		private int columnNumber = 0;
		private int[] columnTypes;
		private String[] columnNames;
		private ColumnBuffer[] columnBuffers;
		
		private int groupSize;
		private int bufferSize;
		private int records = 0;
		private int dataSize = 0;
		private int groupNumber = 0;

		private Map<String, Integer> symbols = new HashMap<String, Integer>();
		
		void writeHeader() throws IOException {
			String magic = "RDX2";
			out.writeBytes(String.format("%s\n", magic));
			
			String format = "X";
			out.writeBytes(String.format("%s\n", format));
			
			int serializationVersion = 2;
			out.writeInt(serializationVersion);
			
			int version = versionNumber(2, 15, 2);
			out.writeInt(version);
			
			int minimalVersion = versionNumber(2, 3, 0);
			out.writeInt(minimalVersion);
			
			out.flush();
		}
		
		static int versionNumber(int v, int p, int s) {
			return ((v * 65536) + (p * 256) + s);
		}


		public Writer(FileSystem fs, Configuration conf, Path path, CompressionCodec codec)
				throws IOException {
			this(fs, conf, path, codec, null, new Metadata());
		}

		public Writer(FileSystem fs, Configuration conf, Path path, CompressionCodec codec, Progressable progress) 
				throws IOException {
			this(fs, conf, path, codec, progress, new Metadata());
		}

		public Writer(FileSystem fs, Configuration conf, Path path, CompressionCodec codec, Progressable progress, Metadata metadata)
				throws IOException {
			this(fs, conf, path, codec, fs.getConf().getInt("io.file.buffer.size", 4096), fs.getDefaultReplication(),
					fs.getDefaultBlockSize(), progress, metadata);
		}

		public Writer(FileSystem fs, Configuration conf, Path path, CompressionCodec codec, int bufferSize, short replication, long blockSize, 
				Progressable progress, Metadata metadata) throws IOException {
			init(conf, path, fs.create(path, true, bufferSize, replication, blockSize, progress), codec, metadata);
			writeHeader();
		}

		void init(Configuration conf, Path path, FSDataOutputStream out, CompressionCodec codec, Metadata metadata) throws IOException {
			this.conf = conf;
			if (codec != null) {
				this.out = new XDROutputStream(codec.createOutputStream(out, CodecPool.getCompressor(codec)));
			} else {
				this.out = new XDROutputStream(out);
			}
			
			this.metadata = metadata;
			
			String fileName = path.getName();
			if (fileName.startsWith("_tmp.")) {
				fileName = fileName.substring(5);
			}
			
			this.name = String.format("%s.%s", RDFile.getDataName(conf), fileName);
			this.columnNumber = RDFile.getColumnNumber(conf);
			this.columnTypes = RDFile.getColumnTypes(conf);
			this.columnNames = RDFile.getColumnNames(conf);

			boolean stringAsFactors = RDFile.isStringsAsFactors(conf);
			this.columnBuffers = new ColumnBuffer[columnNumber];
			for (int i = 0; i < this.columnNumber; i++) {
				switch(columnTypes[i]) {
				case Types.VARCHAR:
					this.columnBuffers[i] = new StringColumnBuffer(columnTypes[i], stringAsFactors);
					break;

				case Types.BOOLEAN:
					this.columnBuffers[i] = new BooleanColumnBuffer(columnTypes[i]);
					break;
					
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
					this.columnBuffers[i] = new IntegerColumnBuffer(columnTypes[i]);
					break;
					
				case Types.FLOAT:
				case Types.DOUBLE:
				case Types.BIGINT:
					this.columnBuffers[i] = new DoubleColumnBuffer(columnTypes[i]);
					break;
					
				case Types.TIMESTAMP:
				default:
					this.columnBuffers[i] = new StringColumnBuffer(columnTypes[i], false);
				}
			}				
			
			this.bufferSize = RDFile.getBufferSize(conf);
			this.groupSize = RDFile.getGroupSize(conf);
		}
		
		public void close() throws IOException {
			if (records > 0) {
				flush();
			}

			out.writeInt(GP.NULL_FLAGS);

			if (out != null) {
				out.flush();
				out.close();
				out = null;
			}
		}

		void clear() {
			for (ColumnBuffer columnBuffer : columnBuffers) {
				try {
					columnBuffer.clear();
				} catch (Exception ignored) { }
			}
		}

		void flush() throws IOException {
			boolean first = groupNumber == 0;
			
			String groupID = String.format("%s.%d", name, groupNumber++);
			out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
			writeSymbol(out, groupID, symbols);
			
			out.writeInt(GP.flags(Type.VEC, 0x0, true, true, false));
			out.writeInt(columnBuffers.length);

			for (ColumnBuffer columnBuffer : columnBuffers) {
				columnBuffer.write(out);
			}
			
			out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
			writeSymbol(out, "names", symbols);
			
			out.writeInt(GP.flags(Type.STR, 0x0, false, false, false));
			out.writeInt(columnNames.length);

			for (int i = 0; i < columnNames.length; i++) {
				out.writeInt(GP.flags(Type.CHAR, GP.ASCII_BIT_MASK | GP.CACHED_MASK | GP.HASHASH_MASK, false, false, false));
				out.writeInt(columnNames[i].length());
				out.writeBytes(columnNames[i]);
			}
			
			out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
			writeSymbol(out, "row.names", symbols);
			
			out.writeInt(GP.flags(Type.INT, 0x0, false, false, false));
			out.writeInt(2);
			out.writeInt(GP.NA_INTEGER);
			out.writeInt(-1 * records);
			
			out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
			writeSymbol(out, "class", symbols);
				
			out.writeInt(GP.flags(Type.STR, 0x0, false, false, false));
			out.writeInt(1);
			out.writeInt(GP.flags(Type.CHAR, GP.ASCII_BIT_MASK | GP.CACHED_MASK | GP.HASHASH_MASK, false, false, false));
			out.writeInt(10);
			out.writeBytes("data.frame");
			
			out.writeInt(GP.NULL_FLAGS);
			
			out.flush();
			
			clear();
			records = 0;
			dataSize = 0;
		}

		static void writeSymbol(DataOutputStream out, String symbol, Map<String, Integer> symbols) throws IOException {
			Integer refObj = symbols.get(symbol);
			if (refObj == null) {
				out.writeInt(GP.flags(Type.SYM, 0x0, false, false, false));
				out.writeInt(GP.flags(Type.CHAR, GP.ASCII_BIT_MASK | GP.CACHED_MASK | GP.HASHASH_MASK, false, false, false));
				out.writeInt(symbol.length());
				out.writeBytes(symbol);
				
				int ref = 0xff | ((symbols.size() + 1) << 8);
				symbols.put(symbol, ref);
			} else {
				out.writeInt(refObj.intValue());
			}
		}
		
		public void append(Writable value) throws IOException {
			if (!(value instanceof BytesRefArrayWritable)) {
				throw new UnsupportedOperationException("Currently the writer can only accept BytesRefArrayWritable");
			}
			
			BytesRefArrayWritable columns = (BytesRefArrayWritable) value;
			int size = columns.size();
			for (int i = 0; i < size; i++) {
				BytesRefWritable column = columns.get(i);
				dataSize += column.getLength();
				columnBuffers[i].append(column);
			}

			if (size < columnNumber) {
				for (int i = columns.size(); i < columnNumber; i++) {
					columnBuffers[i].append(BytesRefWritable.ZeroBytesRefWritable);
				}
			}
		     
			records++;
		    
			if ((dataSize > bufferSize) || (records >= groupSize)) {
				flush();
			}
		}
		
		abstract class ColumnBuffer {
			protected int columnType;
			protected ByteArrayOutputStream valueBuffer;
			protected XDRFilter filter;
			protected int filledSize = 0;
			
			ColumnBuffer(int columnType) {
				this.columnType = columnType;
				this.valueBuffer = new ByteArrayOutputStream(4*1024*1024);
				this.filter = new XDRFilter(valueBuffer);
			}
			
			void clear() throws IOException {
				valueBuffer.reset();
				filter.close();
				filledSize = 0;
			}

			long getLength() throws IOException {
				return filter.getBytesWritten();
			}
			
			abstract void append(BytesRefWritable data) throws IOException;
			abstract void write(DataOutputStream out) throws IOException;
		}
		
		class StringColumnBuffer extends ColumnBuffer {
			private boolean stringsAsFactors = true;
			private Map<String, Integer> factorMap = new LinkedHashMap<String, Integer>();
			
			StringColumnBuffer(int columnType, boolean stringAsFactors) {
				super(columnType);
				this.stringsAsFactors = stringAsFactors;
			}
			
			@Override
			void append(BytesRefWritable data) throws IOException {
				String s = Text.decode(data.getData(), data.getStart(), data.getLength());
				append(s);
			}
			
			void append(String s) throws IOException {
				if (stringsAsFactors) {
					Integer v = factorMap.get(s);
					
					int value = 0;
					if (v != null) {
						value = v.intValue();
					} else {
						value = factorMap.size() + 1;
						factorMap.put(s, Integer.valueOf(value));
					}
					
					filter.writeInt(value);

				} else {
					filter.writeInt(GP.flags(Type.CHAR, GP.UTF8_BIT_MASK | GP.CACHED_MASK, false, false, false));
					if (s == null || s.length() == 0) {
						filter.writeInt(-1);
					} else {
						filter.writeInt(Text.utf8Length(s));
						filter.write(s.getBytes("utf8"));
					}
				}
				
				filledSize++;
			}
			
			@Override
			void write(DataOutputStream out) throws IOException {
				if (stringsAsFactors) {
					out.writeInt(GP.flags(Type.INT, 0x0, true, true, false));
				} else {
					out.writeInt(GP.flags(Type.STR, 0x0, false, false, false));
				}

				out.writeInt(filledSize);
				out.write(valueBuffer.toByteArray());
				
				if (stringsAsFactors) {
					out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
					writeSymbol(out, "levels", symbols);
					
					out.writeInt(GP.flags(Type.STR, 0x0, false, false, false));
					out.writeInt(factorMap.size());
					
					Iterator<String> iterator = factorMap.keySet().iterator();
					while (iterator.hasNext()) {
						String level = (String) iterator.next();
						out.writeInt(GP.flags(Type.CHAR, GP.UTF8_BIT_MASK | GP.CACHED_MASK, false, false, false));
						if (level == null || level.length() == 0) {
							out.writeInt(-1);
						} else {
							out.writeInt(Text.utf8Length(level));
							out.write(level.getBytes("utf8"));
						}
					}
					
					out.writeInt(GP.flags(Type.LIST, 0x0, false, false, true));
					writeSymbol(out, "class", symbols);
					
					out.writeInt(GP.flags(Type.STR, 0x0, false, false, false));
					out.writeInt(1);
					out.writeInt(GP.flags(Type.CHAR, GP.ASCII_BIT_MASK | GP.CACHED_MASK | GP.HASHASH_MASK, false, false, false));
					out.writeInt(6);
					out.writeBytes("factor");
					out.writeInt(GP.NULL_FLAGS);
				}
			}
		}
		
		class BooleanColumnBuffer extends ColumnBuffer {
			BooleanColumnBuffer(int columnType) {
				super(columnType);
			}
			
			@Override
			void append(BytesRefWritable data) throws IOException {
				String s = Text.decode(data.getData(), data.getStart(), data.getLength());
				if (s == null) {
					filter.writeInt(GP.NA_INTEGER);
				} else {
					filter.writeInt(Boolean.parseBoolean(s) == true ? 1 : 0);
				}

				filledSize++;
			}

			@Override
			void write(DataOutputStream out) throws IOException {
				out.writeInt(GP.flags(Type.LGC, 0x0, false, false, false));
				out.writeInt(filledSize);
				out.write(valueBuffer.toByteArray());
			}
		}
		
		class IntegerColumnBuffer extends ColumnBuffer {

			IntegerColumnBuffer(int columnType) {
				super(columnType);
			}
			
			@Override
			void append(BytesRefWritable data) throws IOException {
				String s = Text.decode(data.getData(), data.getStart(), data.getLength());
				if (s == null) {
					filter.writeInt(GP.NA_INTEGER);
				} else {
					filter.writeInt(Integer.parseInt(s));
				}
				
				filledSize++;
			}

			@Override
			void write(DataOutputStream out) throws IOException {
				out.writeInt(GP.flags(Type.INT, 0x0, false, false, false));
				out.writeInt(filledSize);
				out.write(valueBuffer.toByteArray());
			}
		}
		
		class DoubleColumnBuffer extends ColumnBuffer {

			DoubleColumnBuffer(int columnType) {
				super(columnType);
			}
			
			@Override
			void append(BytesRefWritable data) throws IOException {
				String s = Text.decode(data.getData(), data.getStart(), data.getLength());
				if (s == null) {
					filter.writeInt(GP.NA_REAL_HW);
					filter.writeInt(GP.NA_REAL_LW);
				} else {
					filter.writeDouble(Double.parseDouble(s));
				}
				
				filledSize++;
			}

			@Override
			void write(DataOutputStream out) throws IOException {
				out.writeInt(GP.flags(Type.REAL, 0x0, false, false, false));
				out.writeInt(filledSize);
				out.write(valueBuffer.toByteArray());
			}
		}
	}

	
	public static void setBufferSize(Configuration conf, int bufferSize) {
		conf.setInt("dr.hive.io.rcfile.record.buffer.size", bufferSize);
	}

	public static int getBufferSize(Configuration conf) {
		return conf.getInt("dr.hive.io.rcfile.record.buffer.size", 64*1024*1024);
	}

	public static void setGroupSize(Configuration conf, int groupSize) {
		conf.setInt("dr.hive.io.rdfile.group.size", groupSize);
	}
	
	public static int getGroupSize(Configuration conf) {
		return conf.getInt("dr.hive.io.rdfile.group.size", Integer.MAX_VALUE);
	}

	public static void setColumnTypes(Configuration conf, String columnTypes) {
		conf.set("dr.hive.io.rdfile.columns.types", columnTypes);
	}

	public static int[] getColumnTypes(Configuration conf) throws IOException {
		String[] ss = conf.get("dr.hive.io.rdfile.columns.types", "").split(":");
		int types[] = new int[ss.length];
		for (int i = 0; i < ss.length; i++) {
			try {
				types[i] = Utils.hiveTypeToSqlType(ss[i]);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
		
		return types;
	}
	
	public static void setColumnNames(Configuration conf, String columns) {
		conf.set("dr.hive.io.rdfile.columns.names", columns);
	}

	public static String[] getColumnNames(Configuration conf) {
		String ss = conf.get("dr.hive.io.rdfile.columns.names", "");
		return ss.split(",");
	}

	public static void setColumnNumber(Configuration conf, int columnNum) {
		assert columnNum > 0;
		conf.setInt("dr.hive.io.rdfile.column.number", columnNum);
	}
	
	public static int getColumnNumber(Configuration conf) {
		return conf.getInt("dr.hive.io.rdfile.column.number", 0);
	}
	
	public static void setDataName(Configuration conf, String name) {
		conf.set("dr.hive.io.rdfile.data.name", name);	
	}

	public static String getDataName(Configuration conf) {
		return conf.get("dr.hive.io.rdfile.data.name", "dr.hive.io.data.frame");	
	}

	public static void setStringsAsFactors(Configuration conf, boolean stringsAsFactors) {
		conf.setBoolean("dr.hive.io.rdfile.columns.string-as-factors", stringsAsFactors);	
	}
	
	public static boolean isStringsAsFactors(Configuration conf) {
		return conf.getBoolean("dr.hive.io.rdfile.columns.string-as-factors", true);	
	}
}