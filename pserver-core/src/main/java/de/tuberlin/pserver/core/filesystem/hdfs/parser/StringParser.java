package de.tuberlin.pserver.core.filesystem.hdfs.parser;

public class StringParser extends FieldParser<String> {
	
	private static final byte WHITESPACE_SPACE = (byte) ' ';
	private static final byte WHITESPACE_TAB = (byte) '\t';
	
	private static final byte QUOTE_DOUBLE = (byte) '"';
	
	private String result;
	
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, char delim, String reusable) {
		
		int i = startPos;
		
		final byte delByte = (byte) delim;
		byte current;
		
		// count initial whitespace lines
		while (i < limit && ((current = bytes[i]) == WHITESPACE_SPACE || current == WHITESPACE_TAB)) {
			i++;
		}
		
		// first none whitespace character
		if (i < limit && bytes[i] == QUOTE_DOUBLE) {
			// quoted string
			i++; // the quote
			
			// we count only from after the quote
			int quoteStart = i;
			while (i < limit && bytes[i] != QUOTE_DOUBLE) {
				i++;
			}
			
			if (i < limit) {
				// end of the string
				this.result = new String(bytes, quoteStart, i-quoteStart);
				
				i++; // the quote
				
				// skip trailing whitespace characters 
				while (i < limit && (current = bytes[i]) != delByte) {
					if (current == WHITESPACE_SPACE || current == WHITESPACE_TAB) {
						i++;
					}
					else {
						setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
						return -1;	// illegal case of non-whitespace characters trailing
					}
				}
				
				return (i == limit ? limit : i+1);
			} else {
				// exited due to line end without quote termination
				setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
				return -1;
			}
		}
		else {
			// unquoted string
			while (i < limit && bytes[i] != delByte) {
				i++;
			}
			
			// set from the beginning. unquoted strings include the leading whitespaces
			this.result = new String(bytes, startPos, i-startPos);
			return (i == limit ? limit : i+1);
		}
	}
	
	@Override
	public String createValue() {
		return "";
	}

	@Override
	public String getLastResult() {
		return this.result;
	}
}
