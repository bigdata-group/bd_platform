package com.bigdata.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 常用工具类
 * 
 * @author jiwla
 *
 */
public class UtilTools {

	private static final int RADIX = 16;
	private static final String SEED = "0933910847463829827159347601486730416058";
	private static final String[] hexDigits = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d",
			"e", "f" };

	private UtilTools() {
	}

	/**
	 * 获取当前时间 时间格式为yyyy-MM-dd HH:mm:ss
	 * 
	 * @return
	 */
	public static String getCurrentTime() {
		Date now = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String nowTime = sdf.format(now);
		return nowTime;
	}

	/**
	 * 返回一个对象的json字符串
	 * 
	 * @param data
	 * @return
	 */
	public static String toJSON(Object data) {
		ObjectMapper om = new ObjectMapper();
		try {
			return om.writeValueAsString(data);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
	
	 
    public static byte[] convertObjectToJsonBytes(Object object) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper.writeValueAsBytes(object);
    }

	public static <T> T json2Object(String content, Class<T> clazz) {
		ObjectMapper om = new ObjectMapper();
		try {
			return om.readValue(content, clazz);
		} catch (IOException e) {
			throw new RuntimeException("convert json to object error",e);
		}

	}

	public static String join(Collection<String> args, String spliter) {
		if (args != null && !args.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			for (Iterator<String> it = args.iterator(); it.hasNext();) {
				sb.append(it.next());
				if (it.hasNext()) {
					sb.append(spliter);
				}
			}
			return sb.toString();
		} else {
			return null;
		}
	}

	public static String join(String[] args, String spliter) {
		if (args != null && args.length > 0) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < args.length; i++) {
				sb.append(args[i]);
				if (i != args.length - 1) {
					sb.append(spliter);
				}
			}
			return sb.toString();
		} else {
			return null;
		}
	}

	public static String toString(Object obj, String def) {
		if (obj == null) {
			return def;
		} else {
			return ObjectUtils.toString(obj);
		}
	}

	public static final String encryptPassword(String password) {
		if (password == null)
			return "";
		if (password.length() == 0)
			return "";

		BigInteger bi_passwd = new BigInteger(password.getBytes());

		BigInteger bi_r0 = new BigInteger(SEED);
		BigInteger bi_r1 = bi_r0.xor(bi_passwd);

		return bi_r1.toString(RADIX);
	}

	public static final String decryptPassword(String encrypted) {
		if (encrypted == null)
			return "";
		if (encrypted.length() == 0)
			return "";

		BigInteger bi_confuse = new BigInteger(SEED);

		BigInteger bi_r1 = new BigInteger(encrypted, RADIX);
		BigInteger bi_r0 = bi_r1.xor(bi_confuse);

		return new String(bi_r0.toByteArray());
	}

	public String serialize(java.io.Serializable obj) {
		if (obj instanceof String || obj instanceof CharSequence) {
			return obj.toString();
		} else {

			try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(1024);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteStream);) {
				objectOutputStream.writeObject(obj);
				objectOutputStream.flush();
				Base64 base64 = new Base64();
				return base64.encodeToString(byteStream.toByteArray());
			} catch (IOException e) {
				throw new RuntimeException("Object to base64 string error",e);
			}
		}
	}

	public static String generateMultiValueInExpr(String field, String[] items) {
		if (StringUtils.isEmpty(field) || items == null || items.length < 1) {
			return "";
		}
		return generateMultiValueInExpr(field, Arrays.asList(items));
	}

	public static String generateMultiValueInExpr(String field, Collection<String> items) {
		if (StringUtils.isEmpty(field) || items == null || items.size() < 1) {
			return "";
		}
		StringBuilder sb = new StringBuilder(String.format("%s in (", field));
		int count = 1;
		for (String item : items) {
			if (StringUtils.isEmpty(item)) {
				continue;
			} else if (item.indexOf('\'') >= 0) {
				item = item.replace("'", "''");
			}
			if (count <= 1000) {
				sb.append(String.format("'%s',", item));

			} else {
				count = 1;
				sb.setLength(sb.length() - 1);
				sb.append(String.format(") or %s in ('%s',", field, item));
			}
			count++;
		}
		if (count > 1) {
			sb.setLength(sb.length() - 1);
			sb.append(")");
		}
		return sb.toString();
	}

	public static boolean isEmptyStr(String str) {
		return (str == null) || (str.trim().length() == 0);
	}

	public static boolean isDevelopMode() {
		return "true".equalsIgnoreCase(System.getProperty("isDevelopMode"));
	}

	public static String getAEHome() {
		String aeHomePath = null;
		try {//安全问题
			aeHomePath = Objects.toString(MethodUtils.invokeStaticMethod(System.class, "getProperty", new Object[]{"ae_home"}));
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			//AELogger.error(e);
		}
		if (isEmptyStr(aeHomePath)) {
			throw new RuntimeException("ae_home is null, pls set ae_home value.");
		}
		return aeHomePath;
	}
	
	public static String getAEHomeConfig() {
		String aeHomePath = null;
		try {//安全问题
			aeHomePath = Objects.toString(MethodUtils.invokeStaticMethod(System.class, "getProperty", new Object[]{"ae_home_config"}));
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			//AELogger.error(e);
		}
		if (isEmptyStr(aeHomePath)) {
			throw new RuntimeException("ae_home_config is null, please set ae_home_config value.");
		}
		return aeHomePath;
	}

	/**
	 * 字符串s，是否能匹配通配符p，?表示一个字符，*表示0个或多个字符
	 * 
	 * @param s
	 * @param p
	 * @return
	 */
	public static boolean isMatch(String s, String p) {
		if (p == null)
			return false;
		// without this optimization, it will fail for large data set
		int plenNoStar = 0;
		for (char c : p.toCharArray())
			if (c != '*')
				plenNoStar++;
		if (plenNoStar > s.length())
			return false;

		s = " " + s;
		p = " " + p;
		int slen = s.length();
		int plen = p.length();

		boolean[] dp = new boolean[slen];
		TreeSet<Integer> firstTrueSet = new TreeSet<Integer>();
		firstTrueSet.add(0);
		dp[0] = true;

		boolean allStar = true;
		for (int pi = 1; pi < plen; pi++) {
			if (p.charAt(pi) != '*')
				allStar = false;
			for (int si = slen - 1; si >= 0; si--) {
				if (si == 0) {
					dp[si] = allStar ? true : false;
				} else if (p.charAt(pi) != '*') {
					if (s.charAt(si) == p.charAt(pi) || p.charAt(pi) == '?')
						dp[si] = dp[si - 1];
					else
						dp[si] = false;
				} else {
					int firstTruePos = firstTrueSet.isEmpty() ? Integer.MAX_VALUE : firstTrueSet.first();
					if (si >= firstTruePos)
						dp[si] = true;
					else
						dp[si] = false;
				}
				if (dp[si])
					firstTrueSet.add(si);
				else
					firstTrueSet.remove(si);
			}
		}
		return dp[slen - 1];
	}

	/**
	 * 将格式日期转为ms值
	 * 
	 * @param formateDate
	 *            类似于yyyy-MM-dd的日期格式的值
	 * @return
	 */
	public static Long getMsFromDate(String formateDate) {
		Date parse = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parse = sdf.parse(formateDate);
			return parse.getTime();
		} catch (ParseException e) {
			//AELogger.error("Convert date to ms occur error", e);
		}
		return -1L;
	}

	/**
	 * 将int类型数组转为String数组
	 * 
	 * @param intArr
	 *            int类型数组
	 * @return 得到String数组
	 */
	public static String[] convertArrayIntToString(int[] intArr) {
		// List<int[]> list = Arrays.asList(intArr);
		// return (String[])list.toArray(new String[intArr.length]);
		if (intArr == null)
			return null;
		String[] result = new String[intArr.length];
		for (int i = 0; i < intArr.length; i++) {
			result[i] = String.valueOf(intArr[i]);
		}
		return result;
	}
	
	private static String byteArrayToHexString(byte[] b) {
	    StringBuffer resultSb = new StringBuffer();
	    for (int i = 0; i < b.length; i++) {
	      resultSb.append(byteToHexString(b[i]));
	    }
	    return resultSb.toString();
	}

	private static String byteToHexString(byte b) {
		int n = b;
		if (n < 0)
			n = 256 + n;
		int d1 = n / 16;
		int d2 = n % 16;
		return hexDigits[d1] + hexDigits[d2];
	}

	/**
	 * 将字符串加密成32位
	 * @param origin
	 * @return
	 * @since 1.0.0.06
	 */
	public static String encode32(String origin) {
		String resultString = null;
		resultString = new String(origin);
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");//NOSONAR
			resultString = byteArrayToHexString(md.digest(resultString.getBytes()));
		} catch (NoSuchAlgorithmException e) {
			//AELogger.error(e);
		}
		return resultString;
	}

	public static URL stringToURL(String pathOrURL)
			throws MalformedURLException {
		URL result = null;
		if (!UtilTools.isEmptyStr(pathOrURL)) {
			if (pathOrURL.toLowerCase().startsWith("http://")
					|| pathOrURL.toLowerCase().startsWith("file://")) {
				result = new URL(pathOrURL);
			} else {
				String c = "file://" + pathOrURL;
				result = new URL(c);
			}
		}
		return result;
	}

}
