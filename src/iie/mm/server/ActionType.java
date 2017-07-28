package iie.mm.server;

/**
 * //给客户端提供了get和search两种方法读取图片,但是到服务端都对应的是search
 */
public class ActionType {
    public static final byte SYNCSTORE = 1;
    // key based search
    public static final byte SEARCH = 2;
    public static final byte BSEARCH = 12;
    public static final byte DELSET = 3;
    public static final byte ASYNCSTORE = 4;
    public static final byte SERVERINFO = 5;
    // send request and return, wait reply in total
    public static final byte IGET = 6;
    public static final byte MPUT = 7;
    // feature based search
    public static final byte FEATURESEARCH = 8;
    // secondary level search
    public static final byte XSEARCH = 9;
    public static final byte DELETE = 10;
    public static final byte GETINFO = 11;
}
