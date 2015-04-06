package de.tuberlin.pserver.core.filesystem.hdfs.parser;

import java.util.HashMap;
import java.util.Map;

public abstract class FieldParser<T> {

	public static enum ParseErrorState {
		NONE,
		
		NUMERIC_VALUE_OVERFLOW_UNDERFLOW,
		
		NUMERIC_VALUE_ORPHAN_SIGN,
		
		NUMERIC_VALUE_ILLEGAL_CHARACTER,
		
		NUMERIC_VALUE_FORMAT_ERROR,
		
		UNTERMINATED_QUOTED_STRING,
		
		UNQUOTED_CHARS_AFTER_QUOTED_STRING
	}
	
	private ParseErrorState errorState = ParseErrorState.NONE;

	public abstract int parseField(byte[] bytes, int startPos, int limit, char delim, T reuse);

	public abstract T getLastResult();

	public abstract T createValue();

	protected void setErrorState(ParseErrorState error) {
		this.errorState = error;
	}

	public ParseErrorState getErrorState() {
		return this.errorState;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Mapping from types to parsers
	// --------------------------------------------------------------------------------------------

	public static <T> Class<FieldParser<T>> getParserForType(Class<T> type) {
		Class<? extends FieldParser<?>> parser = PARSERS.get(type);
		if (parser == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			Class<FieldParser<T>> typedParser = (Class<FieldParser<T>>) parser;
			return typedParser;
		}
	}
	
	private static final Map<Class<?>, Class<? extends FieldParser<?>>> PARSERS = 
			new HashMap<Class<?>, Class<? extends FieldParser<?>>>();
	
	static {
		// basic types
		PARSERS.put(Byte.class, ByteParser.class);
		PARSERS.put(Short.class, ShortParser.class);
		PARSERS.put(Integer.class, IntParser.class);
		PARSERS.put(Long.class, LongParser.class);
		PARSERS.put(String.class, StringParser.class);
		PARSERS.put(Float.class, FloatParser.class);
		PARSERS.put(Double.class, DoubleParser.class);
	}
}
