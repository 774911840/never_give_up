package com.hery.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/**
 * @author Mars9527
 * @create 2019-06-21 15:21
 **/
public class KrbUtils {

    public static void krbAuth(String user, String keytab, Configuration conf) throws Exception {

        System.setProperty("java.security.krb5.conf", getClassPathFile("krb5.conf"));
        UserGroupInformation.setConfiguration(conf);
//        UserGroupInformation.loginUserFromKeytab(user, keytab);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab);
        UserGroupInformation.setLoginUser(ugi);
    }

    public static void listDir(Configuration conf, String dir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus files[] = fs.listStatus(new Path(dir));
        for (FileStatus file : files) {
            System.out.println(file.getPath());
        }
    }

    public static void auth() throws Exception {
        String user = "koala@QXFDATA.COM";
        String keytab = getClassPathFile("koala.keytab");

        Configuration conf = new Configuration();
        conf.addResource(new Path(getClassPathFile("hdfs-site.xml")));
        conf.addResource(new Path(getClassPathFile("core-site.xml")));
        conf.addResource(new Path(getClassPathFile("yarn-site.xml")));
        krbAuth(user, keytab, conf);
    }

    public static Configuration getConfiguration() {
        String user = "koala@QXFDATA.COM";
        String keytab = getClassPathFile("koala.keytab");
        String dir = "hdfs://qxfdata/";
        Configuration conf = new Configuration();
        conf.addResource(new Path(getClassPathFile("core-site.xml")));
        conf.addResource(new Path(getClassPathFile("hdfs-site.xml")));
        try {
            krbAuth(user, keytab, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conf;
    }


    private static String getClassPathFile(String file) {

        return System.getProperty("user.dir") + "/src/main/resources/"+file;
    }





    public static void main(String[] args) throws IOException {
        String user = "koala@QXFDATA.COM";
        String keytab = getClassPathFile("koala.keytab");
        String dir = "hdfs://qxfdata/";
        Configuration conf = new Configuration();
        conf.addResource(new Path(getClassPathFile("core-site.xml")));
        conf.addResource(new Path(getClassPathFile("hdfs-site.xml")));
        try {
            krbAuth(user, keytab, conf);
            listDir(conf, dir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}