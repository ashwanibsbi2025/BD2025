--ER DIAGRAM CREATION COMMAND

Table Customer {
  CustomerID int [pk]
  Name varchar(100)
  Email varchar(100)
  Gender varchar(10)
}

Table SIM_Card {
  SIMID int [pk]
  PhoneNumber varchar(15)
  ActivationDate date
  Status varchar(20)
  CustomerID int [ref: > Customer.CustomerID]
}

Table Plan {
  PlanID int [pk]
  PlanName varchar(50)
  Type varchar(20)
  Price decimal(10,2)
}

Table Subscription {
  SubscriptionID int [pk]
  SIMID int [ref: > SIM_Card.SIMID]
  PlanID int [ref: > Plan.PlanID]
  StartDate date
  EndDate date
}

Table UsageLog {
  LogID int [pk]
  SIMID int [ref: > SIM_Card.SIMID]
  UsageDate date
  DataUsedMB decimal(10,2)
  VoiceMinutes decimal(10,2)
  SMSCount int
  TowerID int
}

Table Billing {
  BillingID int [pk]
  SubscriptionID int [ref: > Subscription.SubscriptionID]
  BillingDate date
  Amount decimal(10,2)
}

Table NetworkTower {
  TowerID int [pk]
  SiteName varchar(100)
  Region varchar(50)
}

Table SupportTicket {
  TicketID int [pk]
  CustomerID int [ref: > Customer.CustomerID]
  IssueType varchar(50)
  Description text
  Status varchar(20)
  CreatedAt datetime
}
---------------------------------------------------------------------
DROP DATABASE IF EXISTS BharatTelecom;
CREATE DATABASE BharatTelecom;
USE BharatTelecom;

---------------------------------------------------------------
-- Create Customer table
CREATE TABLE Customer (
  CustomerID INT PRIMARY KEY,
  Name VARCHAR(100),
  Email VARCHAR(100),
  Phone VARCHAR(20)
);

-- Create SIM_Card table
CREATE TABLE SIM_Card (
  SIMID INT PRIMARY KEY,
  ActivationDate DATE,
  Status VARCHAR(20),
  CustomerID INT,
  FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

-- Create Plan table
CREATE TABLE Plan (
  PlanID INT PRIMARY KEY,
  PlanName VARCHAR(50),
  Type VARCHAR(20),
  Price DECIMAL(10,2)
);

-- Create Subscription table
CREATE TABLE Subscription (
  SubscriptionID INT PRIMARY KEY,
  SIMID INT,
  PlanID INT,
  StartDate DATE,
  EndDate DATE,
  FOREIGN KEY (SIMID) REFERENCES SIM_Card(SIMID),
  FOREIGN KEY (PlanID) REFERENCES Plan(PlanID)
);

-- Create Billing table
CREATE TABLE Billing (
  BillingID INT PRIMARY KEY,
  SubscriptionID INT,
  Amount DECIMAL(10,2),
  BillingDate DATE,
  FOREIGN KEY (SubscriptionID) REFERENCES Subscription(SubscriptionID)
);

-- Create NetworkTower table
CREATE TABLE NetworkTower (
  TowerID INT PRIMARY KEY,
  SiteName VARCHAR(100),
  Region VARCHAR(100),
  Latitude DECIMAL(9,6),
  Longitude DECIMAL(9,6),
  CoverageRadius DECIMAL(5,2),
  Height DECIMAL(5,2)
);

-- Create UsageLog table
CREATE TABLE UsageLog (
  UsageID INT PRIMARY KEY,
  SIMID INT,
  TowerID INT,
  UsageDate DATE,
  DataUsedMB DECIMAL(8,2),
  VoiceMinutes DECIMAL(6,2),
  SMSCount INT,
  FOREIGN KEY (SIMID) REFERENCES SIM_Card(SIMID),
  FOREIGN KEY (TowerID) REFERENCES NetworkTower(TowerID)
);

-- Create SupportTicket table
CREATE TABLE SupportTicket (
  TicketID INT PRIMARY KEY,
  CustomerID INT,
  IssueType VARCHAR(100),
  OpenedAt DATETIME,
  ClosedAt DATETIME,
  Channel VARCHAR(50),
  Status VARCHAR(50),
  FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

----------------------------------------------------------------------
-- Customer
INSERT INTO Customer VALUES
(1, 'Ananya Singh', 'ananya.singh@example.com', '9876543210'),
(2, 'Ravi Mehra', 'ravi.mehra@example.com', '9823456721'),
(3, 'Sneha Rao', 'sneha.rao@example.com', '9898765432'),
(4, 'Amit Joshi', 'amit.joshi@example.com', '9811112233'),
(5, 'Priya Desai', 'priya.desai@example.com', '9845678901'),
(6, 'Dev Patel', 'dev.patel@example.com', '9797979797');

-- SIM_Card
INSERT INTO SIM_Card VALUES
(101, '2024-12-10', 'Active', 1),
(102, '2025-01-05', 'Suspended', 2),
(103, '2025-02-15', 'Active', 3),
(104, '2025-03-01', 'Active', 4),
(105, '2025-03-20', 'Inactive', 5),
(106, '2025-03-25', 'Active', 6);

-- Plan
INSERT INTO Plan VALUES
(201, 'TalkTime 249', 'Prepaid', 249.00),
(202, 'Unlimited Plus 799', 'Postpaid', 799.00),
(203, 'Data Booster 99', 'Prepaid', 99.00),
(204, 'Family Pack 1099', 'Postpaid', 1099.00),
(205, 'Youth Plan 199', 'Prepaid', 199.00),
(206, 'Pro Surf 599', 'Postpaid', 599.00);

-- Subscription
INSERT INTO Subscription VALUES
(301, 101, 201, '2025-04-01', NULL),
(302, 102, 202, '2025-03-10', '2025-04-10'),
(303, 103, 203, '2025-04-05', NULL),
(304, 104, 204, '2025-03-15', NULL),
(305, 105, 205, '2025-04-01', NULL),
(306, 106, 206, '2025-04-10', NULL);

-- Billing
INSERT INTO Billing VALUES
(401, 301, 249.00, '2025-04-01'),
(402, 302, 799.00, '2025-04-10'),
(403, 303, 99.00, '2025-04-15'),
(404, 304, 1099.00, '2025-04-18'),
(405, 305, 199.00, '2025-04-20'),
(406, 306, 599.00, '2025-04-25');

-- NetworkTower
INSERT INTO NetworkTower VALUES
(501, 'BT-Azura Central', 'Mumbai', 19.0760, 72.8777, 5.00, 35.0),
(502, 'BT-Techhub East', 'Delhi', 28.7041, 77.1025, 6.00, 40.0),
(503, 'BT-Zenith Ridge', 'Bangalore', 12.9716, 77.5946, 4.50, 28.0),
(504, 'BT-Ocean View', 'Chennai', 13.0827, 80.2707, 5.50, 33.0),
(505, 'BT-Valley Link', 'Pune', 18.5204, 73.8567, 6.20, 38.0),
(506, 'BT-EaglePoint', 'Hyderabad', 17.3850, 78.4867, 5.75, 30.0);

-- UsageLog
INSERT INTO UsageLog VALUES
(601, 101, 501, '2025-04-03', 850.50, 75.00, 30),
(602, 102, 502, '2025-04-04', 430.25, 45.00, 15),
(603, 103, 503, '2025-04-07', 1025.75, 120.50, 50),
(604, 104, 504, '2025-04-10', 675.00, 60.00, 25),
(605, 105, 505, '2025-04-12', 300.00, 35.25, 10),
(606, 106, 506, '2025-04-15', 955.90, 82.30, 22);

-- SupportTicket
INSERT INTO SupportTicket VALUES
(701, 1, 'Signal issue in South Mumbai', '2025-04-01 10:30:00', '2025-04-02 16:45:00', 'App', 'Resolved'),
(702, 2, 'Double billing for March', '2025-04-03 11:00:00', '2025-04-04 09:15:00', 'Email', 'Resolved'),
(703, 3, 'SIM activation delay', '2025-04-05 14:20:00', '2025-04-06 13:10:00', 'Phone', 'Closed'),
(704, 4, 'Poor data speed', '2025-04-07 09:50:00', NULL, 'Chat', 'Pending'),
(705, 5, 'Porting status not updated', '2025-04-08 15:40:00', NULL, 'App', 'Open'),
(706, 6, 'Roaming not working abroad', '2025-04-09 17:20:00', '2025-04-10 10:25:00', 'Phone', 'Resolved');

----------------------------------------------------------------------------------------------------
SELECT 
  c.CustomerID, 
  c.Name, 
  c.Email, 
  sc.SIMID, 
  sc.Status, 
  sc.ActivationDate
FROM Customer c
JOIN SIM_Card sc ON c.CustomerID = sc.CustomerID;
--------------------------------------------------------------
SELECT 
  p.PlanID, 
  p.PlanName, 
  p.Type, 
  p.Price, 
  COUNT(s.SubscriptionID) AS ActiveSubscriptions
FROM Plan p
LEFT JOIN Subscription s ON p.PlanID = s.PlanID
WHERE s.EndDate IS NULL
GROUP BY p.PlanID, p.PlanName, p.Type, p.Price;
--------------------------------------------------------------
SELECT 
  nt.TowerID, 
  nt.SiteName, 
  SUM(u.DataUsedMB) AS TotalDataUsed, 
  SUM(u.VoiceMinutes) AS TotalVoiceMinutes, 
  AVG(u.SMSCount) AS AvgSMSCount
FROM NetworkTower nt
JOIN UsageLog u ON nt.TowerID = u.TowerID
GROUP BY nt.TowerID, nt.SiteName;
----------------------------------------------------------------
SELECT 
  b.BillingID, 
  c.CustomerID, 
  c.Name, 
  p.PlanName, 
  b.BillingDate,
  b.Amount AS BilledAmount,
  COALESCE(u.TotalUsageMB, 0) AS TotalDataUsageMB
FROM Billing b
JOIN Subscription s ON b.SubscriptionID = s.SubscriptionID
JOIN SIM_Card sc ON s.SIMID = sc.SIMID
JOIN Customer c ON sc.CustomerID = c.CustomerID
JOIN Plan p ON s.PlanID = p.PlanID
LEFT JOIN (
   SELECT 
     SIMID, 
     SUM(DataUsedMB) AS TotalUsageMB 
   FROM UsageLog 
   WHERE MONTH(UsageDate) = 4 AND YEAR(UsageDate) = 2025
   GROUP BY SIMID
) AS u ON sc.SIMID = u.SIMID
WHERE MONTH(b.BillingDate) = 4 AND YEAR(b.BillingDate) = 2025;
---------------------------------------------------------------------------
SELECT 
  cust.CustomerID, 
  cust.Name, 
  usage_summary.TotalDataUsage
FROM Customer cust
JOIN (
   SELECT 
     sc.CustomerID, 
     SUM(u.DataUsedMB) AS TotalDataUsage
   FROM UsageLog u
   JOIN SIM_Card sc ON u.SIMID = sc.SIMID
   GROUP BY sc.CustomerID
) AS usage_summary 
ON cust.CustomerID = usage_summary.CustomerID
WHERE usage_summary.TotalDataUsage > (
   SELECT AVG(TotalDataUsage) FROM (
      SELECT 
        sc.CustomerID, 
        SUM(u.DataUsedMB) AS TotalDataUsage
      FROM UsageLog u
      JOIN SIM_Card sc ON u.SIMID = sc.SIMID
      GROUP BY sc.CustomerID
   ) AS avg_usage
);
--------------------------------------------------------------------------------
SELECT 
  DAYNAME(UsageDate) AS DayOfWeek, 
  AVG(VoiceMinutes) AS AverageVoiceMinutes
FROM UsageLog
GROUP BY DayOfWeek;
---------------------------------------------------------------------------------
