package LogParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: IsChinese
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 21:57 2021/2/1
 * @Version: 1.0
 * @Modified By:HongLiang
 */
public class IsChinese {
    public static boolean isContainChinese(String str) {
        if (str == null) return false;
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        return m.find();
    }
}
