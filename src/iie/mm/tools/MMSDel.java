package iie.mm.tools;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2016/11/25.
 */
public class MMSDel {

    public static class Option {
        String flag, opt;

        public Option(String flag, String opt) {
            this.flag = flag;
            this.opt = opt;
        }
    }

    public static void main(String[] args) throws ParseException {

        List<String> argsList = new ArrayList<String>();
        List<Option> optsList = new ArrayList<Option>();
        List<String> doubleOptsList = new ArrayList<String>();

        // parse the args
        for (int i = 0; i < args.length; i++) {
            System.out.println("Args " + i + ", " + args[i]);
            switch (args[i].charAt(0)) {
                case '-':
                    if (args[i].length() < 2)
                        throw new IllegalArgumentException("Not a valid argument: " + args[i]);
                    if (args[i].charAt(1) == '-') {
                        if (args[i].length() < 3)
                            throw new IllegalArgumentException("Not a valid argument: " + args[i]);
                        doubleOptsList.add(args[i].substring(2, args[i].length()));
                    } else {
                        if (args.length - 1 > i)
                            if (args[i + 1].charAt(0) == '-') {
                                optsList.add(new Option(args[i], null));
                            } else {
                                optsList.add(new Option(args[i], args[i + 1]));
                                i++;
                            }
                        else {
                            optsList.add(new Option(args[i], null));
                        }
                    }
                    break;
                default:
                    // arg
                    argsList.add(args[i]);
                    break;
            }
        }

        long set_start = -1L;
        long set_end = -1L;


        for (Option o : optsList) {
            if (o.flag.equals("-h")) {
                System.out.println("-h     : print this help.");

            }
            if (o.flag.equals("-set_start")) {
                if (o.opt == null) {
                    System.out.println("-set_start set_start");
                    System.exit(0);
                }
                try {
                    set_start = Long.parseLong(o.opt);
                } catch (NumberFormatException e) {
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    set_start = df.parse(o.opt).getTime();
                }
            }if (o.flag.equals("-set_end")) {
                if (o.opt == null) {
                    System.out.println("-set_end set_end");
                    System.exit(0);
                }
                try {
                    set_end = Long.parseLong(o.opt);
                } catch (NumberFormatException e) {
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    set_end = df.parse(o.opt).getTime();
                }
            }
            if (o.flag.equals("-delh")) {
                ;//set时间段删除redis和disk上的数据
                //1.时间段包含的set
                int is = (int)((set_end - set_start) / 3600);
                List<Long> ss = new ArrayList<Long>();
                for (int i = 0; i < is; i++) {
                   ss.add(set_start + 3600);
                }
                //获取disk路径
                //2。删除disk的数据
                //3。删除redis元数据
            }
        }

    }
}
