package org.neo4j.util;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class StoreCopy
{

    public static void main( String[] args ) throws Exception
    {
        String sourceDir = args[0];
        String targetDir = args[1];
        String sourceConfigFile = args[2];
        String targetConfigFile = args[3];
        copyStore( new File( sourceDir ), new File( targetDir ), new File( sourceConfigFile ), new File(
                targetConfigFile ) );
    }

    private static void copyStore( File sourceDir, File targetDir, File sourceConfigFile, File targetConfigFile )
            throws Exception
    {
        if ( targetDir.exists() )
        {
            throw new IllegalArgumentException( "Target already exists: " + targetDir );
        }
        if ( !sourceDir.exists() )
        {
            throw new IllegalArgumentException( "Source does not exist: " + sourceDir );
        }
        if ( !sourceConfigFile.exists() )
        {
            throw new IllegalArgumentException( "Source config does not exist: " + sourceConfigFile );
        }
        if ( !targetConfigFile.exists() )
        {
            throw new IllegalArgumentException( "Target config does not exist: " + targetConfigFile );
        }

        GraphDatabaseService sourceDb = new EmbeddedGraphDatabase( sourceDir.getAbsolutePath(),
                EmbeddedGraphDatabase.loadConfigurations( sourceConfigFile.getAbsolutePath() ) );
        BatchInserter targetDb = new BatchInserterImpl( targetDir.getAbsolutePath(),
                EmbeddedGraphDatabase.loadConfigurations( targetConfigFile.getAbsolutePath() ) );

        copyNodes( sourceDb, targetDb );
        copyRelationships( sourceDb, targetDb, Boolean.getBoolean( "neo4j.incoming" ) ? new HashSet<Long>() : null );

        targetDb.shutdown();
        sourceDb.shutdown();
//        copyIndex( source, target );
        System.out.println("Done.");
    }

    private static void copyNodes( GraphDatabaseService sourceDb, BatchInserter targetDb )
    {
        System.out.println( "Copying nodes" );
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

    private static void copyRelationships( GraphDatabaseService sourceDb, BatchInserter targetDb, Set<Long> seen )
    {
        System.out.println( "Copying relationships" );
        int count = 0;
        for ( Node node : sourceDb.getAllNodes() )
        {
            for ( Relationship sourceRel : node.getRelationships( seen == null ? Direction.OUTGOING : Direction.BOTH ) )
            {
                if ( seen == null || seen.add( sourceRel.getId() ) )
                {
                    targetDb.createRelationship( sourceRel.getStartNode().getId(), sourceRel.getEndNode().getId(),
                            sourceRel.getType(), getProperties( sourceRel ) );
                }
                count++;
                if ( count % 1000 == 0 ) System.out.print( "." );
                if ( count % 50000 == 0 ) System.out.println( " " + count );
            }
        }
    }

    // private static void copyIndex( File source, File target ) throws IOException
    // {
    // File indexFile = new File( source, "index.db" );
    // if ( indexFile.exists() )
    // {
    // FileUtils.copyFile( indexFile, new File( target, "index.db" ) );
    // }
    // File indexDir = new File( source, "index" );
    // if ( indexDir.exists() )
    // {
    // FileUtils.copyDirectory( indexDir, new File( target, "index" ) );
    // }
    // }

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
