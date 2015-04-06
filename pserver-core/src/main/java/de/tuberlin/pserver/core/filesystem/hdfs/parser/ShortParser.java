package de.tuberlin.pserver.core.filesystem.hdfs.parser;


public class ShortParser extends FieldParser<Short> {
	
	private static final int OVERFLOW_BOUND = 0x7fff;
	private static final int UNDERFLOW_BOUND = 0x8000;
	
	private short result;

	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delimiter, Short reusable) {
		int val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			
			// check for empty field with only the sign
			if (startPos == limit || bytes[startPos] == delimiter) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
				return -1;
			}
		}
		
		for (int i = startPos; i < limit; i++) {
			if (bytes[i] == delimiter) {
				this.result = (short) (neg ? -val : val);
				return i+1;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
			
			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
				return -1;
			}
		}
		
		this.result = (short) (neg ? -val : val);
		return limit;
	}
	
	@Override
	public Short createValue() {
		return Short.MIN_VALUE;
	}

	@Override
	public Short getLastResult() {
		return Short.valueOf(this.result);
	}

	public static final short parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	public static final short parseField(byte[] bytes, int startPos, int length, char delimiter) {
		if (length <= 0) {
			throw new NumberFormatException("Invalid input: Empty string");
		}
		long val = 0;
		boolean neg = false;
		
		if (bytes[startPos] == '-') {
			neg = true;
			startPos++;
			length--;
			if (length == 0 || bytes[startPos] == delimiter) {
				throw new NumberFormatException("Orphaned minus sign.");
			}
		}
		
		for (; length > 0; startPos++, length--) {
			if (bytes[startPos] == delimiter) {
				return (short) (neg ? -val : val);
			}
			if (bytes[startPos] < 48 || bytes[startPos] > 57) {
				throw new NumberFormatException("Invalid character.");
			}
			val *= 10;
			val += bytes[startPos] - 48;
			
			if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
				throw new NumberFormatException("Value overflow/underflow");
			}
		}
		return (short) (neg ? -val : val);
	}
}
