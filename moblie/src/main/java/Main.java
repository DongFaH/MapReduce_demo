import Conf.SingleConfiguration;
import Person.PersonItem;
import Person.PersonPurchase;
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
        PersonItem personItem = new PersonItem();
        Job personItemJob = personItem.main();
        if (driver(personItemJob, "person/item")) System.out.println("personItemComplete");
        PersonPurchase personPurchase = new PersonPurchase();
        Job personPurchaseJob = personPurchase.main();
        if (driver(personPurchaseJob, "person/purchase")) System.out.println("personPurchaseComplete");
        System.exit(0);
    }

    public static boolean driver(Job job, String s) throws IOException, ClassNotFoundException, InterruptedException {

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("/taobao/input/taobao.csv"));
        Path path = new Path("/taobao/output/" + s);
        FileSystem fileSystem = path.getFileSystem(new SingleConfiguration().getConfiguration());
        if (fileSystem.exists(path)) fileSystem.delete(path, true);
        FileOutputFormat.setOutputPath(job, path);

        // 5 执行
        return job.waitForCompletion(true) ? true : false;
    }
}
