/** 
 *@FileName: EmptyUtils.java
 *@Date: 2018年5月9日  
 */

package org.apache.spark.common.utils;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

public class EmptyUtils
{

	public static boolean isEmpty(Object o) throws IllegalArgumentException {
		if (o == null) {
			return true;
		}
		if ((o instanceof String)) {
			return ((String) o).length() == 0;
		} else if ((o instanceof Collection)) {
			return ((Collection<?>) o).isEmpty();
		} else if (o.getClass().isArray()) {
			return Array.getLength(o) == 0;
		} else if ((o instanceof Map)) {
			return ((Map<?, ?>) o).isEmpty();
		} else {
			return false;
		}
	}

	public static boolean isNotEmpty(Object o) {
		return !isEmpty(o);
	}

}
