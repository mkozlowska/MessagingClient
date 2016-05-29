package com.taskorganizer.model;


import javax.xml.bind.annotation.XmlRootElement;
import java.time.Instant;

/**
 * Created by Gosia on 2016-04-26.
 */
@XmlRootElement
public class TaskModel {
    private String name;
    private String id;
    private String description;
    //@JsonFormat(pattern = "yyyy-MM-dd HH:mm")
    private Instant dueDate;
    private Boolean isDone;
    private String userName;

    public TaskModel(String name, String description, String id, Instant dueDate, String userName) {
        this.name = name;
        this.id = id;
        this.description = description;
        this.dueDate = dueDate;
        this.isDone = false;
        this.userName = userName;
    }

    public TaskModel() {
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Instant getDueDate() {
        return dueDate;
    }

    public void setDueDate(Instant dueDate) {
        this.dueDate = dueDate;
    }

    public Boolean getDone() {
        return isDone;
    }

    public void setDone(Boolean done) {
        isDone = done;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TaskModel taskModel = (TaskModel) o;

        if (!name.equals(taskModel.name)) return false;
        if (!id.equals(taskModel.id)) return false;
        if (!description.equals(taskModel.description)) return false;
        if (!dueDate.equals(taskModel.dueDate)) return false;
        if (!isDone.equals(taskModel.isDone)) return false;
        return userName.equals(taskModel.userName);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

}
