package LogParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @ClassName: Parser
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 19:57 2021/2/1
 * @Version: 1.0
 * @Modified By:HongLiang
 */
public class Parser {
    private String remote_addr;// 记录客户端的ip地址
    private String remote_user;// 记录客户端用户名称,忽略属性"-"
    private String time_local;// 记录访问时间与时区
    private String request;// 记录请求的url与http协议
    private String status;// 记录请求状态；成功是200
    private String page;// 记录是哪个页面
    private boolean valid = true;

    public Parser(String s) {
        String[] arr = s.split(" ");
        this.remote_addr = arr[0];
        this.remote_user = arr[1];
        this.time_local = arr[3].substring(1);
        if (!arr[5].equals("\"-\"")) {
            this.request = arr[5].substring(1);
            try {
                arr[6] = arr[6].replaceAll("%(?![0-9a-fA-F]{2})", "%25")
                        .replaceAll("\\+", "%2B");
                this.page = java.net.URLDecoder.decode(arr[6], "utf-8");
            } catch (UnsupportedEncodingException e) {
                this.valid = false;
            }
            this.status = arr[8];
        } else {
            this.valid = false;
            this.status = arr[6];
        }
    }


    public String getRemote_addr() {
        return remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public String getStatus() {
        return status;
    }

    public String getRequest() {
        return request;
    }

    public String getTime_local() {
        return time_local;
    }

    public Date getTime_local_Date() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        return df.parse(this.time_local);
    }

    public String getTime_local_hour() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
        return df.format(this.getTime_local_Date());
    }

    public String getPage() {
        if (IsChinese.isContainChinese(page)) return page;
        return null;
    }

    public boolean getValid() {
        return valid;
    }

    @Override
    public String toString() {
        return "Parser{" +
                "remote_addr='" + remote_addr + '\'' +
                ", remote_user='" + remote_user + '\'' +
                ", time_local='" + time_local + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", page='" + page + '\'' +
                ", valid=" + valid +
                '}';
    }

    public static void main(String[] args) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader("E:\\hadoop\\data\\log\\localhost_access_log.2021-02-01.txt"));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            Parser parser = new Parser(line);
            if (parser.getValid()) {
                try {
                    System.out.println(parser.getTime_local_hour());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

        }
        bufferedReader.close();
    }
}
