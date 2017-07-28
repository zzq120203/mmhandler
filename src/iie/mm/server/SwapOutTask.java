package iie.mm.server;

public class SwapOutTask {

    private String set;
    private String seq;
    private String s;

    public String getSeq() {
        return seq;
    }

    public void setSeq(String seq) {
        this.seq = seq;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }


    public SwapOutTask(String set, String seq, String s) {
        super();
        this.set = set;
        this.seq = seq;
        this.s = s;
    }

    public String getSet() {
        return set;
    }

    public void setSet(String set) {
        this.set = set;
    }

}
