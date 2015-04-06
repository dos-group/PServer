package de.tuberlin.pserver.core.filesystem.hdfs.parser;

/**
 * Parses a text field into a Double.
 */
public class DoubleParser extends FieldParser<Double> {
	
	private static final Double DOUBLE_INSTANCE = Double.valueOf(0.0);
	
	private double result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delimiter, Double reusable) {
		int i = startPos;
		final byte delByte = (byte) delimiter;
		
		while (i < limit && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i-startPos);
		try {
			this.result = Double.parseDouble(str);
			return (i == limit) ? limit : i+1;
		}
		catch (NumberFormatException e) {
			setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
			return -1;
		}
	}
	
	@Override
	public Double createValue() {
		return DOUBLE_INSTANCE;
	}

	@Override
	public Double getLastResult() {
		return Double.valueOf(this.result);
	}

	public static final double parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	public static final double parseField(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		int i = 0;
		final byte delByte = (byte) delimiter;
		
		while (i < length && bytes[i] != delByte) {
			i++;
		}
		
		String str = new String(bytes, startPos, i);
		return Double.parseDouble(str);
	}
}
