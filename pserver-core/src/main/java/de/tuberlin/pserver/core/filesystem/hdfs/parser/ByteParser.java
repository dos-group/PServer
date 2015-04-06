package de.tuberlin.pserver.core.filesystem.hdfs.parser;


public class ByteParser extends FieldParser<Byte> {
	
	private byte result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delimiter, Byte reusable) {
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
				this.result = (byte) (neg ? -val : val);
				return i+1;
			}
			if (bytes[i] < 48 || bytes[i] > 57) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
				return -1;
			}
			val *= 10;
			val += bytes[i] - 48;
			
			if (val > Byte.MAX_VALUE && (!neg || val > -Byte.MIN_VALUE)) {
				setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
				return -1;
			}
		}
		
		this.result = (byte) (neg ? -val : val);
		return limit;
	}
	
	@Override
	public Byte createValue() {
		return Byte.MIN_VALUE;
	}

	@Override
	public Byte getLastResult() {
		return Byte.valueOf(this.result);
	}

	public static final byte parseField(byte[] bytes, int startPos, int length) {
		return parseField(bytes, startPos, length, (char) 0xffff);
	}

	public static final byte parseField(byte[] bytes, int startPos, int length, char delimiter) {
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
				return (byte) (neg ? -val : val);
			}
			if (bytes[startPos] < 48 || bytes[startPos] > 57) {
				throw new NumberFormatException("Invalid character.");
			}
			val *= 10;
			val += bytes[startPos] - 48;
			
			if (val > Byte.MAX_VALUE && (!neg || val > -Byte.MIN_VALUE)) {
				throw new NumberFormatException("Value overflow/underflow");
			}
		}
		return (byte) (neg ? -val : val);
	}
}
