package org.neo4j.util;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;

public class CountInOutRels
{

    private static long relCount;
    private static long nodeCount;

    public static void main( String[] args )
    {
        if (args.length != 1)
        {
            usage();
        }
        String storeDir = args[0];
        if ( !new File( storeDir ).exists() )
        {
            usage();
        }
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( storeDir );
        for ( Node node : graphDb.getAllNodes() )
        {
            nodeCount++;
            for ( Relationship relationship : node.getRelationships( Direction.BOTH ) )
            {
                relCount++;
            }
            if ( nodeCount % 1000 == 0) System.out.print(".");
            if ( nodeCount % 20000 == 0) System.out.println( " " + status() );
        }
        graphDb.shutdown();
        System.out.println( "Done: " + status() );
    }

    private static String status()
    {
        return String.format( "processed %d nodes and %d relationships", nodeCount, relCount );
    }

    private static void usage()
    {
        throw new IllegalArgumentException( "Store dir must be specified." );
    }

}
