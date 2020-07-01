import java.io.Serializable;
import java.util.Date;

public class LogFact implements Serializable {
    private int docId;
    private String office;
    private String user;
    private String day;
    private long time;
    private String log;

    public LogFact(int docId, String office, String user, String day, Date time, String log) {
        this.docId = docId;
        this.office = office;
        this.user = user;
        this.day = day;
        this.time = time.getTime();
        this.log = log;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public void setOffice(String office) {
        this.office = office;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public int getDocId() {
        return docId;
    }

    public String getOffice() {
        return office;
    }

    public String getUser() {
        return user;
    }

    public String getDay() {
        return day;
    }

    public long getTime() {
        return time;
    }

    public String getLog() {
        return log;
    }

    @Override
    public String toString() {
        return "LogFact{" +
                "office='" + office + '\'' +
                ", userName='" + user + '\'' +
                ", monthDay=" + day +
                ", time=" + time +
                ", log='" + log + '\'' +
                '}';
    }
}
