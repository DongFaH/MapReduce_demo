import Analysis.PV;
import Conf.SingleConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Project Name: moblie
 * File Name: Main
 * Package Name: PACKAGE_NAME
 * Date: 2021/1/27 下午9:02
 * Copyright (c) 2021,All Rights Reserved.
 * User: HongLiang
 */

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        PV pv = new PV();
        Job pvJob = pv.main();
        if (driver(pvJob, "PV")) System.out.println("PVComplete");
//        IP ip = new IP();
//        Job ipJob = ip.main();
//        if (driver(ipJob, "IP")) System.out.println("IPComplete");
//        TIME time = new TIME();
//        Job timeJob = time.main();
//        if (driver(timeJob, "TIME")) System.out.println("TIMEComplete");
        System.exit(0);
    }

    public static boolean driver(Job job, String s) throws IOException, ClassNotFoundException, InterruptedException {

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/logs/input/logs.txt"));
        Path path = new Path("/logs/output/" + s);
        FileSystem fileSystem = path.getFileSystem(new SingleConfiguration().getConfiguration());
        if (fileSystem.exists(path)) fileSystem.delete(path, true);
        FileOutputFormat.setOutputPath(job, path);

        // 5 执行
        return job.waitForCompletion(true);
    }
}
