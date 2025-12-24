/*
=====================================================
Create Database
=====================================================
Script Purpose:
	This script creates a new database named 'datawarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated.
*/
drop database if exists datawarehouse;

create database if not exists datawarehouse;

use datawarehouse;
