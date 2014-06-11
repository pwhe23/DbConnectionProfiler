DbConnectionProfiler
====================

A simple DbConnection wrapper to Debug print all issued sql statements. You only
need to include the DbConnectionProfiler.cs file in your project.

The goal is that you can simply copy the statement from the Visual Studio
output window and paste directly into Sql Management Studio to execute. Currently
there is not complete support for every possible sql construct that can be
executed, however there should be plenty for most projects using stored procedures
or sql statements. Feel free to add anything missing to DbConnectionProfiler.GetCommandText
and send over a pull request.

I pulled most of the code for the profiling stuff from the MiniProfiler/dotnet project,
simply replacing their references to IDbProfiler to my simple one that prints the statements.
https://github.com/MiniProfiler/dotnet
