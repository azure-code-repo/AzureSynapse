foreach (var ds in Model.DataSources.OfType<StructuredDataSource> ())
{
    var user = Environment.GetEnvironmentVariable ("usrvar");
    var pword = Environment.GetEnvironmentVariable ("pwdvar");
    var serverinfo = Environment.GetEnvironmentVariable ("server");
    var db = Environment.GetEnvironmentVariable("database");

    ds.Username = user;
    ds.Password = pword;

    ds.Server = serverinfo;
    ds.Database = db;

    Console.WriteLine(user);
    Console.WriteLine(pword);
    Console.WriteLine(serverinfo);
    Console.WriteLine(db);
}
foreach (var role in Model.Roles) {
    // Find all Azure AD role members where MemberID is assigned:
    var orgMembers = role.Members.OfType<ExternalModelRoleMember> ()
        .Where (m => m.IdentityProvider == "AzureAD" && !string.IsNullOrEmpty (m.MemberID)).ToList ();

    // Delete the member and recreate it without assigning MemberID:
    foreach (var orgMember in orgMembers) {
        orgMember.Delete ();
        role.AddExternalMember (orgMember.MemberName);
    }
}
