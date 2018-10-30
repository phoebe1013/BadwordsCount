package bdp.wordcount;

public class JsonObject {
    private String author;
    private String body;
    private String selftext;
    private String subreddit;
    private String title;
    // Add "created_utc"
    private String create;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getSelftext() {
        return selftext;
    }

    public void setSelftext(String selftext) {
        this.selftext = selftext;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getSubreddit() {
        return subreddit;
    }

    public void setSubreddit(String subreddit) {
        this.subreddit = subreddit;
    }

    // Add "create_utc"
    public String getCreate(){  return create; }

    public void setCreate(String create) {  this.create = create; }


}
