package nc.isi.jdbc_sync.utils;

public class StringUtils {

	/**
	 * @return <code>true</code> iff at least one string was appended.
	 */
	public static boolean join(Iterable<String> strings, String joinString,
			StringBuilder buffer) {
		boolean first = true;
		for (String column : strings) {
			if (first) {
				first = false;
			} else {
				buffer.append(joinString);
			}
			buffer.append(column);
		}
		return !first;
	}

	/**
	 * @return <code>strings</code> joined by <code>joinString</code> or an
	 *         empty string if strings is empty.
	 */
	public static String join(Iterable<String> strings, String joinString) {
		StringBuilder buf = new StringBuilder();
		join(strings, joinString, buf);
		return buf.toString();
	}

}
