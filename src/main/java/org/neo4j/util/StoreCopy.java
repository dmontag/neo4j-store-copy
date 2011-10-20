package org.neo4j.util;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class StoreCopy
{

    public static void main( String[] args ) throws Exception
    {
        String sourceDir = args[0];
        String targetDir = args[1];
        String configFile = args[2];
        copyStore( sourceDir, targetDir, configFile );
    }

    private static void copyStore( String sourceDir, String targetDir, String configFile ) throws Exception
    {
        File target = new File( targetDir );
        File source = new File( sourceDir );
        File config = new File( configFile );
        if ( target.exists() )
        {
            throw new IllegalArgumentException( "Target already exists: " + target );
        }
        if ( !source.exists() )
        {
            throw new IllegalArgumentException( "Source does not exist: " + source );
        }
        if ( !config.exists() )
        {
            throw new IllegalArgumentException( "Config does not exist: " + configFile );
        }

        BatchInserter targetDb = new BatchInserterImpl( target.getAbsolutePath() );
        GraphDatabaseService sourceDb = new EmbeddedGraphDatabase( source.getAbsolutePath(),
            EmbeddedGraphDatabase.loadConfigurations( config.getAbsolutePath() ) );

        copyNodes( sourceDb, targetDb );
        copyRelationships( sourceDb, targetDb );

        targetDb.shutdown();
        sourceDb.shutdown();
//        copyIndex( source, target );
    }

    private static void copyNodes( GraphDatabaseService sourceDb, BatchInserter targetDb )
    {
        System.out.println("Copying nodes");
        int count = 0;
        Node referenceNode = sourceDb.getReferenceNode();
        for ( Node sourceNode : sourceDb.getAllNodes() )
        {
            if ( sourceNode.equals( referenceNode ) )
            {
                targetDb.setNodeProperties( targetDb.getReferenceNode(), getProperties( sourceNode ) );
            }
            else
            {
                targetDb.createNode( sourceNode.getId(), getProperties( sourceNode ) );
            }
            count++;
            if ( count % 1000 == 0 ) System.out.print( "." );
            if ( count % 50000 == 0 ) System.out.println( " " + count );
        }
    }

    private static void copyRelationships( GraphDatabaseService sourceDb, BatchInserter targetDb )
    {
        System.out.println("Copying relationships");
        int count = 0;
        for ( Node node : sourceDb.getAllNodes() )
        {
            for ( Relationship sourceRel : node.getRelationships( Direction.OUTGOING ) )
            {
                targetDb.createRelationship( sourceRel.getStartNode().getId(), sourceRel.getEndNode().getId(),
                    sourceRel.getType(), getProperties( sourceRel ) );
                count++;
                if ( count % 1000 == 0 ) System.out.print( "." );
                if ( count % 50000 == 0 ) System.out.println( " " + count );
            }
        }
    }

//    private static void copyIndex( File source, File target ) throws IOException
//    {
//        File indexFile = new File( source, "index.db" );
//        if ( indexFile.exists() )
//        {
//            FileUtils.copyFile( indexFile, new File( target, "index.db" ) );
//        }
//        File indexDir = new File( source, "index" );
//        if ( indexDir.exists() )
//        {
//            FileUtils.copyDirectory( indexDir, new File( target, "index" ) );
//        }
//    }

    private static Map<String, Object> getProperties( PropertyContainer pc )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        for ( String property : pc.getPropertyKeys() )
        {
            result.put( property, pc.getProperty( property ) );
        }
        return result;
    }
}
