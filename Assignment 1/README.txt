Name:Kiran Bhat Gopalakrishna
Net Id:kxb140230

Setup input folders hadoop fs

1)Place frinedlist file i.e soc-LiveJournal1Adj.txt in /user/hduser/input_friendlist
2)Place the userdata file i.e userdata.txt in /user/hduser/input_userdata

Run the following commands

q1)hadoop jar ~/BigData/Hw1Q1.jar Hw1Q1 /user/hduser/input_friendlist /user/hduser/output_intermediate /user/hduser/output_intermediate /user/hduser/output_q1

	Sample o/p : 924	439,2409,6995,11860,15416,43748,45881
				 8941	8938,8939,8942,8945,8946
				 8941	8943,8944,8940
				 8942	8939,8940,8943,8944
				 9019	9022,317,9023
				 9020	9021,9016,9017,9022,317,9023
				 9021	9020,9016,9017,9022,317,9023
				 9022	9019,9020,9021,317,9016,9017,9023
				 9990	13134,13478,13877,34299,34485,34642,37941
				 9992	9987,9989,35667,9991
				 9993	9991,13134,13478,13877,34299,34485,34642,37941

q2)hadoop jar ~/BigData/Hw1Q2.jar Hw1Q2 /user/hduser/input_friendlist /user/hduser/output_hw1q2  14 15

q3) hadoop jar ~/BigData/Hw1Q3.jar Hw1Q3 /user/hduser/input_friendlist /user/hduser/output_intermediate /user/hduser/input_userdata /user/hduser/output_hw1q3  14 15

q4)hadoop jar ~/BigData/Hw1Q4.jar Hw1Q4 /user/hduser/input_friendlist /user/hduser/input_userdata /user/hduser/output_intermediate /user/hduser/output_q4
