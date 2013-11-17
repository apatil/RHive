package com.nexr.rhive.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HiddenPathFilter implements PathFilter {

	@Override
	public boolean accept(Path path) {
		return !isHiddenFile(path);
	}

	private boolean isHiddenFile(Path path) {
		String name = path.getName();
		if (name.startsWith(".") || name.startsWith("_")) {
			return true;
		}
		
		return false;
	}
}
