using System.Collections.Generic;
using System.IO;
using UnityEditor;
using UnityEngine;

public class GenerateWorldCards
{
    [MenuItem("VRChat/Generate World Cards")]
    public static void Generate()
    {
        string path = Path.Combine(Application.dataPath, "approved_export.json");
        if (!File.Exists(path))
        {
            Debug.LogError("approved_export.json not found");
            return;
        }

        string json = File.ReadAllText(path);
        WorldList wrapper = JsonUtility.FromJson<WorldList>("{\"items\":" + json + "}");
        Debug.Log($"Loaded {wrapper.items.Count} worlds.");
        // TODO: instantiate prefab templates here
    }

    [System.Serializable]
    public class WorldInfo
    {
        public string worldId;
        public string name;
        public string author;
        public string imageUrl;
        public string description;
        public string[] tags;
        public int visits;
    }

    [System.Serializable]
    public class WorldList
    {
        public List<WorldInfo> items;
    }
}
