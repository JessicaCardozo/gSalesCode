package dbconn1;

import java.util.Comparator;

public class User implements Comparator<User>{
	String sceId;
	String firstName;
	double salesActPoints;
	double salesOutcomePoints;
	double tatPoints;
	double salesLeaderboardPoints;
	String image;
	
	public String getImage() {
		return image;
	}
	public void setImage(String image) {
		this.image = image;
	}
	public String getSceId() {
		return sceId;
	}
	public void setSceId(String sceId) {
		this.sceId = sceId;
	}
	public double getSalesActPoints() {
		return salesActPoints;
	}
	public void setSalesActPoints(double salesActPoints) {
		this.salesActPoints = salesActPoints;
	}
	public double getSalesOutcomePoints() {
		return salesOutcomePoints;
	}
	public void setSalesOutcomePoints(double salesOutcomePoints) {
		this.salesOutcomePoints = salesOutcomePoints;
	}
	public double getTatPoints() {
		return tatPoints;
	}
	public void setTatPoints(double tatPoints) {
		this.tatPoints = tatPoints;
	}
	public double getSalesLeaderboardPoints() {
		return salesLeaderboardPoints;
	}
	public void setSalesLeaderboardPoints(double salesLeaderboardPoints) {
		this.salesLeaderboardPoints = salesLeaderboardPoints;
	}	
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	@Override
	public int compare(User arg0,User arg1) {

        if (arg0.getSalesLeaderboardPoints() < arg1.getSalesLeaderboardPoints()) {System.out.println("returning -1");return -1;}
        if (arg0.getSalesLeaderboardPoints() > arg1.getSalesLeaderboardPoints()){ System.out.println("returning 1");return 1;}
        return 0;
          
    }
}
